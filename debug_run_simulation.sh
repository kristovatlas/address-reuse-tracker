export no_proxy=* #prevents urllib2 process from silently crashing
python -m unittest discover -p "data_subscription_slow_test.py"
osascript -e 'beep'
