from rest_framework import serializers
from .models import DCountry, DSkill, DSource, DCompany, FJobOffers, FGithubTrends, FSearchTrends, FSurveyResponses

class DCountrySerializer(serializers.ModelSerializer):
    class Meta:
        model = DCountry
        fields = '__all__'

class DSkillSerializer(serializers.ModelSerializer):
    class Meta:
        model = DSkill
        fields = '__all__'

class DSourceSerializer(serializers.ModelSerializer):
    class Meta:
        model = DSource
        fields = '__all__'

class DCompanySerializer(serializers.ModelSerializer):
    class Meta:
        model = DCompany
        fields = '__all__'

class JobOfferSerializer(serializers.ModelSerializer):
    class Meta:
        model = FJobOffers
        fields = '__all__'

class SalaryStatsSerializer(serializers.Serializer):
    country = serializers.CharField()
    skill = serializers.CharField()
    median_salary_eur = serializers.FloatField()
    p25 = serializers.FloatField()
    p75 = serializers.FloatField()
    sample_size = serializers.IntegerField()

class SkillTrendSerializer(serializers.Serializer):
    skill = serializers.CharField()
    popularity_score = serializers.FloatField()
    github_stars = serializers.IntegerField()
    google_interest = serializers.FloatField()
    job_count = serializers.IntegerField()
