import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain.tools import BaseTool
from pydantic import BaseModel, Field
from typing import Type, Optional
from langchain.agents import AgentExecutor, create_react_agent, create_openai_functions_agent
from langchain.prompts import PromptTemplate

load_dotenv()
TABLE_MINUTE = os.getenv("TABLE_MINUTE", "candles_1m")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "market_data")

# ---- Tools ----
class CQLQueryInput(BaseModel):
    query: str = Field(description="CQL to execute. Always filter by partition key (e.g., symbol) and LIMIT.")

class CQLQueryTool(BaseTool):
    name: str = "cql_query"
    description: str = "Execute CQL on Cassandra. Prefer filtering by partition key (symbol) and add LIMIT."
    args_schema: Type[BaseModel] = CQLQueryInput
    def __init__(self, cassandra_db, **kwargs):
        super().__init__(**kwargs); self.cassandra_db = cassandra_db
    def _run(self, query: str) -> str:
        return self.cassandra_db.execute_cql(query)

class CassandraInfoTool(BaseTool):
    name: str = "cassandra_info"
    description: str = "Get keyspace tables and schemas."
    def __init__(self, cassandra_db, **kwargs):
        super().__init__(**kwargs); self.cassandra_db = cassandra_db
    def _run(self, tool_input: str = "") -> str:
        return self.cassandra_db.get_database_info()

# ---- Prompts ----
def react_prompt():
    template = f"""You are a Cassandra/CQL expert for time-series market data in keyspace '{KEYSPACE}'.

Primary table (minute-level): {TABLE_MINUTE}
Typical columns:
- symbol (text) - PARTITION KEY
- day (date)    - CLUSTERING
- time (text)   - CLUSTERING (e.g., 'HH:mm:ss') or single 'datetime' timestamp
- open, high, low, close (double), volume (double)
- optional indicators: ma*, bb_*, macd_*, rsi14, obv, ...

TOOLS:
{{tools}}

RULES:
- Always filter by symbol and (if available) by day range. Always add LIMIT.
- Avoid full scans; ALLOW FILTERING only if dataset is small.
- Briefly explain what you are doing.

FORMAT:
Question: the question
Thought: reasoning
Action: one of [{{tool_names}}]
Action Input: input
Observation: result
... repeat
Thought: final reasoning
Final Answer: concise answer + (optional) short CQL snippet

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
