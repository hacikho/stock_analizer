from app.db import SessionLocal, CanSlimData

session = SessionLocal()
try:
    results = session.query(CanSlimData).order_by(CanSlimData.created_at.desc()).limit(5).all()
    for row in results:
        print(f"ID: {row.id}, Symbol: {row.symbol}, Date: {row.data_date}, Time: {row.data_time}, Created: {row.created_at}")
        print(f"Data: {row.data_json}\n")
finally:
    session.close()
