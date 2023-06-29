import utils.constant as constant
import os

from utils.mqtt_to_db import MQTT_TO_DB
from dotenv import load_dotenv

load_dotenv()

mqtt_to_db = MQTT_TO_DB(
        server=os.getenv('SERVER'),
        database=os.getenv('DATABASE'),
        user_login=os.getenv('USER_LOGIN'),
        password=os.getenv('PASSWORD'),
        table=constant.IRR_CHANGE_BITE_TABLE,
        table_columns=constant.IRR_CHANGE_BITE_TABLE_COLUMNS,
        table_log=constant.IRR_CHANGE_BITE_TABLE_LOG,
        table_columns_log=constant.IRR_CHANGE_BITE_TABLE_COLUMNS_LOG,
        mqtt_topic=constant.MQTT_TOPIC,
        mqtt_broker=os.getenv('MQTT_BROKER'),
        slack_notify_token=os.getenv('SLACK_NOTIFY_TOKEN')
    )

mqtt_to_db.run()