from django.core.management.base import BaseCommand
from django.contrib.auth.models import User
from rest_framework.authtoken.models import Token

class Command(BaseCommand):
    help = 'Create API token for TalentInsight'

    def handle(self, *args, **options):
        username = 'talentinsight_api'
        user, created = User.objects.get_or_create(username=username)
        
        if created:
            user.set_password('talentinsight2025')
            user.save()
            self.stdout.write(f'User {username} created')
        
        token, created = Token.objects.get_or_create(user=user)
        
        if created:
            self.stdout.write(f'Token created: {token.key}')
        else:
            self.stdout.write(f'Existing token: {token.key}')
        
        self.stdout.write('Use this token in Authorization header: Token ' + token.key)
