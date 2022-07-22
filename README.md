# seaflow
The most free process choreographer

<a href="https://rainware.github.io/blogs/project%20documents/2022/07/22/seaflow-guide.html?">Seaflow Guide</a>

# Install
```language=bash
pip install seaflow
```

# Example

Download seaflow examples
```language=bash
git clone https://github.com/rainware/seaflow.git
cd seaflow/examples
```
#### 1. setup db
```language=bash
python setup_db.py
```

#### 2. load actions and dags
```language=bash
python load_actions.py
```
```language=bash
python load_dags.py
```

#### 3. start celery worker
```language=bash
celery -A main worker --concurrency=3 -E -l info
```

#### 4. run tasks
```
python run_tasks.py
```

#### Demo dag and task
<img alt="README-f0076196.png" src="assets/README-f0076196.png" width="" height="" >
