from os import path

from setuptools import find_packages, setup

dirname = path.abspath(path.dirname(__file__))
with open(path.join(dirname, 'README.md')) as f:
    long_description = f.read()

extras_require = {
    'flask': [ 'flask' ],
    'boto3': [ 'boto3', 'botocore' ]
}
extras_require['complete'] = sorted(set(sum(extras_require.values(), [])))

setup(
    name='featrest',
    version='1.0.0',
    packages=find_packages(),
    description='a qufaframework for automated feature engineering',
    url='https://github.com/oslab-ewha/featrest',
    license='BSD 3-clause',
    author='KyungWoon Cho',
    author_email='cezanne@codemayo.com',
    classifiers=[
         'Development Status :: 1 - Alpha',
         'Intended Audience :: Developers',
         'Programming Language :: Python :: 3',
         'Programming Language :: Python :: 3.6',
         'Programming Language :: Python :: 3.7',
         'Programming Language :: Python :: 3.8'
    ],
    install_requires=open('requirements.txt').readlines(),
    python_requires='>=3.6, <4',
    extras_require=extras_require,
    keywords='feature engineering REST API mkfeat',
    include_package_data=True,
    entry_points={
            'console_scripts': [
              'featrest = featrest.__main__:cli'
            ]
    },
    long_description=long_description,
    long_description_content_type='text/markdown'
)
