from django.core.serializers.json import DjangoJSONEncoder
from decimal import Decimal

class MdpJSONEncoder(DjangoJSONEncoder):
    
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super().default(o)