try:
    from Message import MessageDlx
except ModuleNotFoundError:
    print("Importando MessageConsumer")
    from trempy.Messages.Message import MessageDlx

consumer = MessageDlx(task_name="cdc_scd2_employees")

consumer.start_consuming()