import json
import logging
import pypgoutput

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(relativeCreated)6d %(processName)s %(message)s')
    SLOT_NAME = "my_slot"
    DB_NAME = "test"
    LOCAL_DSN = f"host=localhost user=test port=5432 dbname={DB_NAME} password=test"
    cdc_reader = pypgoutput.LogicalReplicationReader(db_name=DB_NAME, db_dsn=LOCAL_DSN, slot_name=SLOT_NAME)
    counter = 0
    messages = []
    for msg in cdc_reader:
        messages.append(msg)
        logger.info(f"Collected: {len(messages)} messages")
        if counter > 9:
            break
        counter += 1
    cdc_reader.stop()

    for x in messages:
        logger.info(json.dumps(msg, indent=2, default=str))
