# setup.py

from setuptools import setup, find_packages

setup(
    name='ocutil',
    version='2.0.3',
    description='Oracle Cloud Object Storage CLI utility',
    author='Shai Nisan',
    author_email='your.email@example.com',
    packages=find_packages(),
    install_requires=[
        'oci',
        'tqdm',
    ],
    entry_points={
        'console_scripts': [
            'ocutil=ocutil.main:main',
        ],
    },
    python_requires='>=3.6',
)
