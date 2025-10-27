from typing import Dict
from airflow.providers.slack.notifications.slack import SlackNotifier

def persist_state_before_failure(error: Exception, context:Dict, metadata:Dict) -> None:
    """
    Persists extraction state before raising an error in case of failure.
    Metadata is provided by the Extractor class attributes
    Exception is the original exc that was raised during extraction.
    """
    state = {
        "last_saved_page": metadata.get("last_saved_page"),
        "last_page_token": metadata.get("last_page_token"),
        "next_page_url": metadata.get("next_page_url"),
    }

    ti = context['task_instance']
    ti.xcom_push(
        key="state_before_failure",
        value=state
    )

    details = (
        "Extraction FAILED\n"
        f"Last saved page: {state['last_saved_page']}\n"
        f"Last saved token: {state['last_page_token']}\n"
        f"ERROR: {error}"
    )

    notifier = SlackNotifier(
        slack_conn_id='slack',
        text=details,
        channel='ct-gov'
    )
    notifier.notify(context)
    raise error

