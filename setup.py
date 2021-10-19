import os, sys
try:
    from setuptools import setup, find_packages
    use_setuptools = True
except ImportError:
    from distutils.core import setup
    use_setuptools = False

try:
    with open('README.rst', 'rt') as readme:
        description = '\n' + readme.read()
except IOError:
    # maybe running setup.py from some other dir
    description = ''

python_requires = '>=3.6'
install_requires = [
    'rx>=3.2',
    'cyclotron>=1.2',
    'aiokafka>=0.7.1',
]

setup(
    name="cyclotron-aiokafka",
    version='0.6.0',
    url='https://github.com/MainRo/cyclotron-aiokafka.git',
    license='MIT',
    description="AioKafka driver for cyclotron",
    long_description=description,
    author='Romain Picard',
    author_email='romain.picard@oakbits.com',
    packages=find_packages(),
    install_requires=install_requires,
    include_package_data=True,
    platforms='any',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3'
    ]
)
