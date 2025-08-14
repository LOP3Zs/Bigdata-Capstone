import os
from dotenv import load_dotenv
from cassandra_conn import CassandraConnection, test_connection
from cql_tools import CassandraDatabase
from agent import CassandraChatbot

load_dotenv()

def boot():
    if not test_connection():
        return None
    session = CassandraConnection().get_session()
    db = CassandraDatabase(session, os.getenv("CASSANDRA_KEYSPACE", "market_data"))
    mode = os.getenv("AGENT_MODE", "react").lower().strip()
    bot = CassandraChatbot(db, mode=mode)
    print(f"🤖 MarketQL ready. Mode={mode}")
    return bot

if __name__ == "__main__":
    bot = boot()
    if not bot:
        exit(1)
    print("Type 'schema' to view database, 'quit' to exit.")
    while True:
        try:
            q = input("\n❓ Your question: ").strip()
            if q.lower() in ("quit", "exit"): break
            if q.lower() == "schema":
                print(bot.ask("Show me the database schema")); continue
            print(bot.ask(q))
        except KeyboardInterrupt:
            break