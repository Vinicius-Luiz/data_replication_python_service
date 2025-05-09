try:
    from Message import MessageConsumer
except ModuleNotFoundError:
    print("Importando MessageConsumer")
    from trempy.Messages.Message import MessageConsumer

consumer = MessageConsumer(task_name="cdc_scd2_employees")

consumer.start_consuming()