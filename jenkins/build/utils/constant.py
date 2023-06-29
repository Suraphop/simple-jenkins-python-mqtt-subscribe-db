# data mqtt_irr
IRR_CHANGE_BITE_TABLE = 'data_irr_change_bite'
IRR_CHANGE_BITE_TABLE_COLUMNS ='''
            registered_at datetime,
            mfg_date date,
            my_str varchar(20),
			temp2 varchar(10),
            tmp1 varchar(10),
            mc_no varchar(10),
            process varchar(10),
            '''
            
IRR_CHANGE_BITE_TABLE_LOG = 'log_irr_change_bite'
IRR_CHANGE_BITE_TABLE_COLUMNS_LOG ='''
            registered_at datetime,
			status varchar(50),
            process varchar(50),
            message varchar(50),
            error varchar(MAX),
            '''

#LOG status
STATUS_OK = 'ok'
STATUS_ERROR = 'error'
STATUS_INFO = 'info'

#MQTT TOPIC
MQTT_TOPIC = "nat/GD/#"