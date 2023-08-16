import pytest

from dcicutils import contribution_scripts as contribution_scripts_module
from dcicutils.command_utils import ScriptFailure
from dcicutils.contribution_scripts import show_contributors, show_contributors_main
from unittest import mock


class MockContributions:

    def __init__(self, repo, exclude_fork=None, verbose=False):
        self.repo = repo
        self.exclude_fork = exclude_fork
        self.verbose = verbose


def test_show_contributors():
    with mock.patch.object(contribution_scripts_module, "Contributions") as mock_contributions:
        contributions_object = mock.MagicMock()
        mock_contributions.return_value = contributions_object

        show_contributors(repo='my-repo')

        assert contributions_object.save_contributor_data.call_count == 0

        mock_contributions.assert_called_once_with(repo='my-repo', exclude_fork=None, verbose=False)
        contributions_object.show_repo_contributors.assert_called_once_with(error_class=None)

        mock_contributions.reset_mock()
        contributions_object = mock.MagicMock()
        mock_contributions.return_value = contributions_object

        show_contributors(repo='another-repo', exclude_fork='whatever', save_contributors=True, verbose=True, test=True)

        mock_contributions.assert_called_once_with(repo='another-repo', exclude_fork='whatever', verbose=True)
        contributions_object.show_repo_contributors.assert_called_once_with(error_class=ScriptFailure)
        contributions_object.save_contributor_data.assert_called_once_with()


def test_show_contributors_main():

    with mock.patch.object(contribution_scripts_module, "show_contributors") as mock_show_contributors:

        with pytest.raises(SystemExit) as exc:

            show_contributors_main(simulated_args=['some-repo'])

        assert exc.value.code == 0

        mock_show_contributors.assert_called_once_with(repo='some-repo', exclude_fork=None, verbose=False,
                                                       save_contributors=False, test=False)

        mock_show_contributors.reset_mock()

        with pytest.raises(SystemExit) as exc:

            show_contributors_main(simulated_args=['my-repo', '--exclude', 'their-repo',
                                                   '--save-contributors', '--test', '--verbose'])

        assert exc.value.code == 0

        mock_show_contributors.assert_called_once_with(repo='my-repo', exclude_fork='their-repo',
                                                       save_contributors=True, test=True, verbose=True)
