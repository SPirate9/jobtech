from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import CountryViewSet, SkillViewSet, SourceViewSet, CompanyViewSet, JobOfferViewSet

router = DefaultRouter()
router.register(r'countries', CountryViewSet)
router.register(r'skills', SkillViewSet)
router.register(r'sources', SourceViewSet)
router.register(r'companies', CompanyViewSet)
router.register(r'jobs', JobOfferViewSet)

urlpatterns = [
    path('api/v1/', include(router.urls)),
]
