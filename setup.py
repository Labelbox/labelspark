from setuptools import find_packages, setup

setup(
    name='labelspark',
    version='0.1.0',
    packages=find_packages(),
    url='https://github.com/Labelbox/LabelSpark.git',
    description='Labelbox & Databricks integration helper library',
    install_requires=[],
    extras_require={
        'dev': ['pylint']
    }
)
