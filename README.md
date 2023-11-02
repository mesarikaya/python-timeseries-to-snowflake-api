# Cockpit

v0.0.1

## Project

- **Project description:** Cockpit Snowflake API
- **Project type:** MVP

## Team

- **Tech Lead:** 
- [Ergin Sarikaya](mailto:ergin_sarikaya@cargill.com 'Email')

## Tech Stack (Tentative)

- Python/Django 
- Authentication using JWT token.  
- Queue messaging system/caching (Redis). 
- KONG API Gateway 
- IAM groups
- Scheduled task queue (Celery) 

## Creating Scheduled Tasks

For creating scheduled tasks, firstly define a regular python method on cockpit-snowflake-api/cp_snowflake/snowflake_drf/tasks.py
```
@shared_task(bind=True)
def one_dummy_task(self):
    pass
```

After that, update the cockpit-snowflake-api/cp_snowflake/cp_snowflake_api/celery.py to create an scheduled task
```
#create a task that runs automatically every 5 minutes
app.conf.beat_schedule = {
    'one_dummy_task':{
        'task':'snowflake_drf.tasks.one_dummy_task',
        'schedule': crontab(minute='*/5'),
    },
    ...
}
```

## Git Flow

Then when it's ready to be merged/tested in the cloud environment: 

local branch -> development ->  stage -> prod
