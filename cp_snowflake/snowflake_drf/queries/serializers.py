from dataclasses import dataclass
from rest_framework.serializers import Serializer, CharField, ListField

@dataclass
class SnowFlakeMTPMRequest(object):
    mtpm: list
    ui_level: str
    datetimestart: str
    datetimeend: str

class SnowFlakeMTPMRequestSerializer(Serializer):
    mtpm = ListField()
    ui_level: CharField()
    datetimestart = CharField()
    datetimeend = CharField()

    def save(self):
        self.mtpm = self.context['mtpm']
        self.ui_level = self.context['ui_level']
        self.datetimestart = self.context['datetimestart']
        self.datetimeend = self.context['datetimeend']

@dataclass
class SnowFlakeBaseResponse:
    results: ListField 

class SnowFlakeBaseResponseSerializer(Serializer):
    results = ListField()

    def create(self, validated_data):
        return SnowFlakeBaseResponse(**validated_data)

    def update(self, instance, validated_data):
        instance.response = validated_data.get('results', instance.results)