from setuptools import setup

setup(name='simple_db_builder',
    version='0.1',
    description='A SQL task manager.',
    author='Edward Nunes',
    author_email='ed.a.nunes@gmail.com',
    url='https://github.com/Nunie123/simple_db_builder',
    packages=['simple_db_builder'],
    install_requires=[
                    'mysql-connector-python==8.0.12',
                    'SQLAlchemy==1.3.0'
                    ],
    classifiers=[
                "Programming Language :: Python :: 3",
                "License :: OSI Approved :: MIT License",
                "Operating System :: OS Independent",
                ],
   )