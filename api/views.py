from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from django.db import connection
from django.db.models import Q, Count, Avg
from drf_spectacular.utils import extend_schema, extend_schema_view, OpenApiParameter
from drf_spectacular.types import OpenApiTypes
from .models import DCountry, DSkill, DSource, DCompany, FJobOffers, FGithubTrends, FSearchTrends, FSurveyResponses
from .serializers import (
    DCountrySerializer, DSkillSerializer, DSourceSerializer, DCompanySerializer,
    JobOfferSerializer, SalaryStatsSerializer, SkillTrendSerializer
)

@extend_schema_view(
    list=extend_schema(description="Liste des pays européens disponibles"),
    retrieve=extend_schema(description="Détails d'un pays spécifique"),
)
class CountryViewSet(viewsets.ReadOnlyModelViewSet):
    """ViewSet pour la gestion des pays européens."""
    queryset = DCountry.objects.all()
    serializer_class = DCountrySerializer

@extend_schema_view(
    list=extend_schema(description="Liste des compétences technologiques"),
    retrieve=extend_schema(description="Détails d'une compétence spécifique"),
)
class SkillViewSet(viewsets.ReadOnlyModelViewSet):
    """ViewSet pour la gestion des compétences technologiques."""
    queryset = DSkill.objects.all()
    serializer_class = DSkillSerializer

@extend_schema_view(
    list=extend_schema(description="Liste des sources de données"),
    retrieve=extend_schema(description="Détails d'une source spécifique"),
)
class SourceViewSet(viewsets.ReadOnlyModelViewSet):
    """ViewSet pour la gestion des sources de données."""
    queryset = DSource.objects.all()
    serializer_class = DSourceSerializer

@extend_schema_view(
    list=extend_schema(description="Liste des entreprises"),
    retrieve=extend_schema(description="Détails d'une entreprise spécifique"),
)
class CompanyViewSet(viewsets.ReadOnlyModelViewSet):
    """ViewSet pour la gestion des entreprises."""
    queryset = DCompany.objects.all()
    serializer_class = DCompanySerializer

@extend_schema_view(
    list=extend_schema(description="Liste des offres d'emploi avec filtrage possible"),
    retrieve=extend_schema(description="Détails d'une offre d'emploi spécifique"),
)
class JobOfferViewSet(viewsets.ReadOnlyModelViewSet):
    """ViewSet pour la gestion des offres d'emploi avec endpoints analytiques."""
    queryset = FJobOffers.objects.all()
    serializer_class = JobOfferSerializer
    
    def get_queryset(self):
        queryset = FJobOffers.objects.all()
        country = self.request.query_params.get('country')
        skill = self.request.query_params.get('skill')
        
        if country:
            country_obj = DCountry.objects.filter(iso2=country).first()
            if country_obj:
                queryset = queryset.filter(id_country=country_obj.id_country)
        
        if skill:
            skill_obj = DSkill.objects.filter(tech_label=skill).first()
            if skill_obj:
                queryset = queryset.filter(id_skill=skill_obj.id_skill)
        
        return queryset

    @extend_schema(
        description="Statistiques salariales quotidiennes par pays et compétence",
        parameters=[
            OpenApiParameter(name='country', description='Code ISO2 du pays (ex: FR)', required=True, type=OpenApiTypes.STR),
            OpenApiParameter(name='skill', description='Nom de la compétence (ex: Python)', required=True, type=OpenApiTypes.STR),
        ],
        responses={200: SalaryStatsSerializer(many=True)}
    )
    @action(detail=False, methods=['get'])
    def salary_daily(self, request):
        country = request.query_params.get('country')
        skill = request.query_params.get('skill')
        
        if not country or not skill:
            return Response(
                {"error": "country and skill parameters are required"}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    c.iso2 as country,
                    s.tech_label as skill,
                    AVG(f.salary_avg) as median_salary_eur,
                    MIN(f.salary_avg) as p25,
                    MAX(f.salary_avg) as p75,
                    COUNT(*) as sample_size
                FROM f_job_offers f
                JOIN d_country c ON f.id_country = c.id_country
                JOIN d_skill s ON f.id_skill = s.id_skill
                WHERE c.iso2 = %s AND s.tech_label = %s 
                AND f.salary_avg IS NOT NULL
                GROUP BY c.iso2, s.tech_label
            """, [country, skill])
            
            result = cursor.fetchone()
            
            if result:
                data = {
                    'country': result[0],
                    'skill': result[1],
                    'median_salary_eur': round(result[2], 2) if result[2] else 0,
                    'p25': round(result[3], 2) if result[3] else 0,
                    'p75': round(result[4], 2) if result[4] else 0,
                    'sample_size': result[5]
                }
                serializer = SalaryStatsSerializer(data)
                return Response(serializer.data)
            else:
                return Response(
                    {"error": "No data found for this country/skill combination"}, 
                    status=status.HTTP_404_NOT_FOUND
                )

    @extend_schema(
        description="Top 10 des tendances de compétences technologiques basées sur GitHub et Google Trends",
        responses={200: SkillTrendSerializer(many=True)}
    )
    @action(detail=False, methods=['get'])
    def skill_trends(self, request):
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    s.tech_label as skill,
                    AVG(gt.popularity_score) as popularity_score,
                    AVG(gt.stars) as github_stars,
                    AVG(goog.interest_value) as google_interest,
                    COUNT(jo.title) as job_count
                FROM d_skill s
                LEFT JOIN f_github_trends gt ON s.id_skill = gt.id_skill
                LEFT JOIN f_search_trends goog ON s.id_skill = goog.id_skill  
                LEFT JOIN f_job_offers jo ON s.id_skill = jo.id_skill
                GROUP BY s.tech_label
                ORDER BY popularity_score DESC
                LIMIT 10
            """)
            
            results = cursor.fetchall()
            data = []
            
            for result in results:
                data.append({
                    'skill': result[0],
                    'popularity_score': round(result[1], 2) if result[1] else 0,
                    'github_stars': int(result[2]) if result[2] else 0,
                    'google_interest': round(result[3], 2) if result[3] else 0,
                    'job_count': result[4]
                })
            
            serializer = SkillTrendSerializer(data, many=True)
            return Response(serializer.data)
