from setuptools import setup

setup(
    name='dataprov',
    version='0.0.3',    
    description='A python library for collecting data provenance in pre-processing pipelines which use pandas DataFrames',
    url='',
    author='Luca Lauro, Giulia Simonelli',
    packages=['dataprov','dataprov.prov_acquisition','dataprov.prov_acquisition.prov_libraries','dataprov.prov_acquisition.real_world_pipeline','dataprov.prov_acquisition.other_pipeline','dataprov.new_queries'],
    install_requires=[  "click==7.1.2",
                        "Flask==1.1.2",
                        "itsdangerous==1.1.0",
                        "Jinja2==2.11.3",
                        "MarkupSafe==1.1.1",
                        "numpy==1.22.3",
                        "pandas==1.2.4",
                        "pymongo==3.11.4",
                        "python-dateutil==2.8.1",
                        "pytz==2021.1",
                        "six==1.15.0",
                        "Werkzeug==1.0.1"
    ]
)
