from setuptools import setup

setup(
    name='ocutil',
    version='0.1.0',
    py_modules=['ocutil'],  # Tells setuptools to treat ocutil.py as a module named 'ocutil'
    install_requires=[
        'oci'
    ],
    entry_points={
        'console_scripts': [
            'ocutil=ocutil:main',  # 'ocutil.py' must have a function named 'main()'
        ]
    },
    python_requires='>=3.7',
    description='A simple CLI to upload files to Oracle Cloud Object Storage'
)
