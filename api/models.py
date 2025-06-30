from django.db import models

class DCountry(models.Model):
    id_country = models.AutoField(primary_key=True)
    iso2 = models.CharField(max_length=2, unique=True)
    country_name = models.TextField(blank=True, null=True)
    region = models.TextField(blank=True, null=True)
    currency = models.CharField(max_length=3, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'd_country'

class DSkill(models.Model):
    id_skill = models.AutoField(primary_key=True)
    skill_group = models.TextField(blank=True, null=True)
    tech_label = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'd_skill'

class DSource(models.Model):
    id_source = models.AutoField(primary_key=True)
    source_name = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'd_source'

class DCompany(models.Model):
    id_company = models.AutoField(primary_key=True)
    company_name = models.TextField(blank=True, null=True)
    sector = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'd_company'

class DDate(models.Model):
    date_key = models.DateField(primary_key=True)
    day = models.IntegerField(blank=True, null=True)
    month = models.IntegerField(blank=True, null=True)
    quarter = models.IntegerField(blank=True, null=True)
    year = models.IntegerField(blank=True, null=True)
    day_week = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'd_date'

class FJobOffers(models.Model):
    # Utilisation du rowid de SQLite comme clé primaire
    id = models.IntegerField(primary_key=True, db_column='rowid')
    date_key = models.CharField(max_length=20, blank=True, null=True)
    id_country = models.IntegerField(blank=True, null=True)
    id_skill = models.IntegerField(blank=True, null=True)
    id_source = models.IntegerField(blank=True, null=True)
    id_company = models.IntegerField(blank=True, null=True)
    title = models.TextField(blank=True, null=True)
    location = models.TextField(blank=True, null=True)
    salary_min = models.FloatField(blank=True, null=True)
    salary_max = models.FloatField(blank=True, null=True)
    salary_avg = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'f_job_offers'

class FGithubTrends(models.Model):
    id_trend = models.AutoField(primary_key=True)
    date_key = models.CharField(max_length=20, blank=True, null=True)
    id_skill = models.IntegerField(blank=True, null=True)
    id_source = models.IntegerField(blank=True, null=True)
    repository_name = models.TextField(blank=True, null=True)
    stars = models.IntegerField(blank=True, null=True)
    forks = models.IntegerField(blank=True, null=True)
    popularity_score = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'f_github_trends'

class FSearchTrends(models.Model):
    id_gtrend = models.AutoField(primary_key=True)
    date_key = models.CharField(max_length=20, blank=True, null=True)
    id_skill = models.IntegerField(blank=True, null=True)
    id_source = models.IntegerField(blank=True, null=True)
    keyword = models.TextField(blank=True, null=True)
    interest_value = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'f_search_trends'

class FSurveyResponses(models.Model):
    id_survey = models.AutoField(primary_key=True)
    id_country = models.IntegerField(blank=True, null=True)
    id_skill = models.IntegerField(blank=True, null=True)
    id_source = models.IntegerField(blank=True, null=True)
    salary = models.FloatField(blank=True, null=True)
    experience_years = models.IntegerField(blank=True, null=True)
    dev_type = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'f_survey_responses'
