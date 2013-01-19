from setuptools import setup, find_packages

setup(
        name = 'flow_workflow',
        version = '0.1',
        packages = find_packages(exclude=['unit_tests']),
        setup_requires = [
            'nose',
        ],
        install_requires = [
            'argparse',
            'redis',
            'lxml',
        ],
        tests_require = [
            'nose',
            'mock',
            'fakeredis',
        ],
        test_suite = 'unit_tests',
)
