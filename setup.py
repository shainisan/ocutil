# setup.py

from setuptools import setup, find_packages

setup(
    name='ocutil',
    version='1.0.0',
    description='Oracle Cloud Object Storage CLI utility',
    author='Your Name',
    author_email='your.email@example.com',
    packages=find_packages(),
    install_requires=[
        'oci',
    ],
    entry_points={
        'console_scripts': [
            'ocutil=ocutil:main',
        ],
    },
    python_requires='>=3.6',
)