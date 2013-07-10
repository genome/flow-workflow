import generator
import os
import sys
import unittest


MODULE = sys.modules[__name__]


def base_dir():
    return os.path.dirname(os.path.abspath(__file__))

def directory(subdir):
    return os.path.join(base_dir(), subdir)


CLASS_GENERATOR = generator.ClassGenerator(
        config_directory=directory('config'),
        perl_directory=directory('perl'),
        workflows_directory=directory('workflows'),
        module=MODULE)

CLASS_GENERATOR.generate_classes()



if __name__ == '__main__':
    unittest.main()
