import json
import traceback

class BusinessError(Exception):
    def __init__(self, e, code):
        # Incluye errorCode
        payload = {
            "errorCode": code,
            "errorMessage": str(e),
            "traceback": traceback.format_exc()
        }
        super().__init__(json.dumps(payload, ensure_ascii=False))

"""
def BusinessError(e, code):
    return {
            "status": "ERROR",
            "errorCode": code,
            "errorMessage": str(e),
            "traceback": traceback.format_exc()
        }
"""