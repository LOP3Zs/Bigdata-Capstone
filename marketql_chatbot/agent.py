import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain.tools import BaseTool
from pydantic import BaseModel, Field
from typing import Type, Optional
from langchain.agents import AgentExecutor, create_react_agent, create_openai_functions_agent
from langchain.prompts import PromptTemplate
from datetime import datetime, timedelta

load_dotenv()
TABLE_MINUTE = os.getenv("TABLE_MINUTE", "candles_1m")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "market_data")

from typing import Optional, Type
from pydantic import BaseModel, Field
from langchain.tools import BaseTool
from cql_tools import CassandraDatabase

# ---- Tools ----
class CQLQueryInput(BaseModel):
    query: str = Field(
        description="CQL to execute. Always filter by partition key (e.g., symbol) and LIMIT."
    )

class CQLQueryTool(BaseTool):
    """Execute raw CQL and pretty-print tabular results if any."""

    name: str = "cql_query"
    description: str = (
        "Run CQL against Cassandra. Use for targeted reads with WHERE on partition keys "
        "and LIMIT to keep results small."
    )
    # KHAI BÁO THUỘC TÍNH LÀ FIELD Pydantic
    cassandra_db: CassandraDatabase
    # Ràng buộc schema tham số
    args_schema: Type[CQLQueryInput] = CQLQueryInput

    class Config:
        arbitrary_types_allowed = True  # cho phép CassandraDatabase làm field

    def _run(self, query: str, run_manager=None) -> str:
        return self.cassandra_db.execute_cql(query)

    async def _arun(self, query: str, run_manager=None) -> str:
        raise NotImplementedError("CQLQueryTool does not support async")

# ---- Info tool (liệt kê bảng / xem schema) ----
class CassandraInfoInput(BaseModel):
    table: Optional[str] = Field(
        default=None,
        description="Table name to describe. If omitted, list all tables."
    )

class CassandraInfoTool(BaseTool):
    name: str = "cassandra_info"
    description: str = (
        "List Cassandra tables in the current keyspace or show the schema of a specific table."
    )
    cassandra_db: CassandraDatabase
    args_schema: Type[CassandraInfoInput] = CassandraInfoInput

    class Config:
        arbitrary_types_allowed = True

    def _run(self, table: Optional[str] = None, run_manager=None) -> str:
        if table:
            return self.cassandra_db.get_table_schema(table)
        tables = self.cassandra_db.get_table_names()
        return "Tables:\n" + "\n".join(f"- {t}" for t in tables)

    async def _arun(self, table: Optional[str] = None, run_manager=None) -> str:
        raise NotImplementedError("CassandraInfoTool does not support async")

# ---- Prompts ----
def react_prompt():
    current_date = datetime.now().strftime('%Y-%m-%d')
    yesterday_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    current_time = datetime.now().strftime('%H:%M:%S')
    template = f"""You are a Cassandra/CQL expert for time-series market data in keyspace '{KEYSPACE}'.

CURRENT CONTEXT:
- Today's date: {current_date} (YYYY-MM-DD format)
- Current time: {current_time} (HH:mm:ss format)
- Timezone: UTC

PRIMARY TABLE: ohlcv_1m
Schema:
- PRIMARY KEY: ((symbol, day), time)
- symbol (text) - PARTITION KEY component
- day (date) - PARTITION KEY component
- time (time) - CLUSTERING KEY (HH:mm:ss format)
- OHLCV: open, high, low, close (float), volume (bigint)
- Technical indicators: ma5, ma10, ma20, ma50, ma100, ma200, bb_*, macd_*, rsi14, adx, stoch_*, cci, atr, etc.

PARTITION KEY STRATEGY:
- ALWAYS specify both symbol AND day in WHERE clause for efficient queries
- For "latest" data: use today's date first, then yesterday if no data
- For "recent" data: query last 3-7 days
- For historical ranges: specify exact date range

QUERY PATTERNS:
1. Latest data (most recent):
   WHERE symbol='SYMBOL' AND day >= '{current_date}' ORDER BY day DESC, time DESC LIMIT 1

2. Today's data:
   WHERE symbol='SYMBOL' AND day='{current_date}' ORDER BY time DESC LIMIT N

3. Date range:
   WHERE symbol='SYMBOL' AND day >= 'START_DATE' AND day <= 'END_DATE' ORDER BY day DESC, time DESC

4. Multiple symbols (same day):
   WHERE symbol IN ('SYM1','SYM2') AND day='{current_date}'

TOOLS:
{{tools}}

RULES:
1. PARTITION EFFICIENCY: Always include symbol + day in WHERE clause
2. TIME CONTEXT: When user asks for "latest/current/now", use today's date
3. FALLBACK STRATEGY: If no data today, try yesterday (day='{yesterday_date}')
4. LIMITS: Always add LIMIT clause (default: 1 for latest, 100 for ranges)
5. AVOID ALLOW FILTERING: Only use if dataset is confirmed small
6. DATE FORMATS: Use 'YYYY-MM-DD' for date, 'HH:mm:ss' for time
7. EXPLAIN: Briefly mention which partition(s) you're querying

TIME INTERPRETATION:
- "latest/current/now" → today's date + ORDER BY time DESC
- "yesterday" → {yesterday_date}
- "last week" → last 7 days range
- "this month" → current month range
- "recent" → last 3 days

FORMAT:
Question: the question
Thought: I need to query partition(s) for [symbol] on [date/range] to get [what data]
Action: one of [{{tool_names}}]
Action Input: SELECT ... FROM ohlcv_1m WHERE symbol='...' AND day='...' ORDER BY ... LIMIT ...
Observation: result
... repeat if needed
Thought: Got the data from partition [symbol,day], this shows [interpretation]
Final Answer: [concise answer] + [CQL snippet if helpful]

Question: {{input}}
Thought: {{agent_scratchpad}}"""
    return PromptTemplate.from_template(template)

def functions_prompt():
    return PromptTemplate.from_template("You are a helpful Cassandra assistant. Use tools when needed.\nQuestion: {input}")

# ---- Builders ----
def build_llm():
    return ChatOpenAI(
        model=os.getenv("MODEL_NAME", "gpt-4o-mini"),
        temperature=float(os.getenv("TEMPERATURE", "0.0")),
        api_key=os.getenv("OPENAI_API_KEY")
    )

def build_react_agent(tools):
    prompt = react_prompt()
    llm = build_llm()
    agent = create_react_agent(llm, tools, prompt)
    return AgentExecutor(agent=agent, tools=tools, verbose=True, handle_parsing_errors=True, max_iterations=15, max_execution_time=300)

def build_functions_agent(tools):
    prompt = functions_prompt()
    llm = build_llm()
    agent = create_openai_functions_agent(llm, tools, prompt)
    return AgentExecutor(agent=agent, tools=tools, verbose=True, handle_parsing_errors=True, max_iterations=8, max_execution_time=120)

# ---- Public API ----
class CassandraChatbot:
    def __init__(self, cassandra_db, mode: Optional[str] = "react"):
        tools = [CQLQueryTool(cassandra_db=cassandra_db), CassandraInfoTool(cassandra_db=cassandra_db)]
        self.executor = build_react_agent(tools) if mode == "react" else build_functions_agent(tools)

    def ask(self, question: str) -> str:
        try:
            result = self.executor.invoke({"input": question})
            return result["output"]
        except Exception as e:
            return f"Error: {e}"