try:
    from Message import MessageProducer
except ModuleNotFoundError:
    print("Importando MessageProducer")
    from trempy.Messages.Message import MessageProducer

producer = MessageProducer(task_name="cdc_scd2_employees")

producer.publish_message(data={"action": "update", "name": "Vinícius Luiz", "id": 5})
