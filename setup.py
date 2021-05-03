from setuptools import find_packages, setup

setup(
    name='labelspark',
    packages=find_packages(),
    url='https://github.com/Labelbox/LabelSpark.git',
    description='Labelbox / Spark integration tools',
    install_requires=[],
    extras_require={
        'dev': ['pylint']
    }
)
