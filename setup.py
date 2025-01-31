from setuptools import setup, find_packages

setup(
    name='ocutil',
    version='0.1.0',
    description='A simple CLI to upload files to Oracle Cloud Object Storage (like gsutil).',
    author='Shai Nisan',
    author_email='never_mind@example.com',
    packages=find_packages(exclude=[]),
    install_requires=[
        'oci'
    ],
    entry_points={
        'console_scripts': [
            'ocutil=ocutil:main'
        ],
    },
    python_requires='>=3.6',
)
