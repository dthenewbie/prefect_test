from prefect.blocks.notifications import SlackWebhook
def slack_webhook_block() -> SlackWebhook:
    slack_webhook_block = SlackWebhook.load("flowcheck")
    return slack_webhook_block