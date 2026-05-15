from sqlalchemy import create_engine, text

e = create_engine('postgresql://trader:password@localhost:5434/fundamentals')
with e.begin() as c:
    try:
        c.execute(text('ALTER TABLE research_hypotheses DROP CONSTRAINT chk_expected_timeframe;'))
    except Exception as ex:
        pass # Might already be dropped
    c.execute(text("ALTER TABLE research_hypotheses ADD CONSTRAINT chk_expected_timeframe CHECK (expected_timeframe IN ('intraday', 'swing', 'position', 'n/a'));"))
    print("Done")
