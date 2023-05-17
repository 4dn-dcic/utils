import pytest

from dcicutils.misc_utils import ignorable
from dcicutils.project_utils import ProjectRegistry, Project


def test_project_registry_register():

    with ProjectRegistry.project_registry_test_context():

        @ProjectRegistry.register('foo')
        class FooProject(Project):
            NAME = "foo"
            PRETTY_NAME = "Fu"

        assert FooProject.NAME == 'foo'
        assert FooProject.PRETTY_NAME == 'Fu'

        @ProjectRegistry.register('foobar')
        class FooBarProject(Project):
            NAME = "foobar"

        assert FooBarProject.NAME == 'foobar'
        assert FooBarProject.PRETTY_NAME == 'Foobar'

        assert FooProject.PYPROJECT_NAME == 'foo'

        with pytest.raises(Exception) as exc:
            @ProjectRegistry.register('foo')
            class FooProject(Project):
                PYPROJECT_NAME = 'foo'
            ignorable(FooProject)  # It won't get this far.
        assert str(exc.value) == ("Explicit FooProject.PYPROJECT_NAME='foo' is not permitted."
                                  " This assignment is intended to be managed implicitly.")

        with pytest.raises(Exception) as exc:
            @ProjectRegistry.register('foobar')
            class FooProject(Project):
                PYPROJECT_NAME = 'foo'
            ignorable(FooProject)  # It won't get this far.
        assert str(exc.value) == ("Explicit FooProject.PYPROJECT_NAME='foo' is not permitted."
                                  " This assignment is intended to be managed implicitly.")
