# this script doesnt work right now
# Todo: create working scripts to run tests from command line (work in progress)
cd ./monetate-recommendations
ls ../venv/monetate-recommendations/bin/activate
pwd
source ../venv/monetate-recommendations/bin/activate
source ../.bashrc
python manage.py test tests.test_view_also_view
