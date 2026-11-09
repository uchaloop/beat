import unittest
from release_notes import extract

MODULE = 'module github.com/uchaloop/beat\n'


class ReleaseNotesTests(unittest.TestCase):
    def test_versions_and_policy(self):
        for tag, expected in [('v0.8.0', True), ('v1.0.0', False), ('v1.1.0-rc.1', True)]:
            with self.subTest(tag=tag):
                notes, prerelease = extract(tag, MODULE, f'## [{tag[1:]}] - 2026-11-09\n\n- Change\n')
                self.assertEqual(notes, '- Change\n')
                self.assertEqual(prerelease, expected)

    def test_invalid_tags(self):
        for tag in ['v2.0.0', 'v01.0.0', 'v1.01.0', 'v1.0', 'v1.0.0-01', 'v1.0.0-', 'v1.0.0+build', 'job/v0.8.0']:
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                extract(tag, MODULE, '')

    def test_invalid_sections(self):
        for text in ['', '## [Unreleased]\n- Change', '## [0x8x0] - 2026-11-09\n- Change',
                     '## [0.8.0] - Unreleased\n- Change', '## [0.8.0] - 2026-02-30\n- Change',
                     '## [0.8.0] - 2026-11-09\n\n### Added\n',
                     '## [0.8.0] - 2026-11-09\n\n[0.8.0]: https://example.com',
                     '## [0.8.0] - 2026-11-09\n- Change\n## [0.8.0] - 2026-11-09\n- Other']:
            with self.subTest(text=text), self.assertRaises(ValueError):
                extract('v0.8.0', MODULE, text)

    def test_exact_section(self):
        notes, _ = extract('v0.8.0', MODULE, '## [Unreleased]\n- Future\n## [0.8.0] - 2026-11-09\n- Current\n## [0.7.1] - 2026-10-01\n- Old\n')
        self.assertEqual(notes, '- Current\n')

    def test_wrong_module(self):
        with self.assertRaises(ValueError):
            extract('v0.8.0', 'module github.com/uchaloop/beat/v2', '')
