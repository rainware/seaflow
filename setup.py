import setuptools

setuptools.setup(
    name="seaflow",
    version="0.0.1-beta-6",
    author="rainware",
    author_email="kevin90116@gmail.com",
    description="the most free process choreographer",
    url="https://github.com/rainware/seaflow",
    project_urls={
        "Source": "https://github.com/rainware/seaflow",
    },
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        'django>=3.1',
        'mysqlclient>=1.4.4',
        'django-extensions>=3.0.3',
        'celery>=5.2.1',
        'django-celery-results>=2.2.0',
        'redis>=3.5.3',
        'flower>=1.0.0',
        'django-mysql>=3.8.1',
        'timeout-decorator>=0.4.1',
        'jsonpath-rw>=1.4.0',
        'json-logic-qubit>=0.9.1'
    ],
    platforms='any'
)
