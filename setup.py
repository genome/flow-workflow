import distribute_setup
distribute_setup.use_setuptools()

from setuptools import setup, find_packages

setup(
        name = 'flow_workflow',
        version = '0.1',
        packages = find_packages(exclude=['unit_tests']),
        entry_points = entry_points,
        install_requires = [
            'argparse',
            'redis',
        ],
        tests_require = [
            'mock',
        ],
        test_suite = 'unit_tests',
)
