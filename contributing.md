# Guidance on how to contribute

> All contributions to this project will be released under an MIT open-source license.
> By submitting a pull request or filing a bug, issue, or
> feature request, you are agreeing to comply with this [LICENSE](LICENSE).
> Our intent is to support the community -- please contact us if you need help getting involved.

There are two primary ways to help:
 - Using the issue tracker, and
 - Changing the code-base.


## Using the issue tracker

Use the issue tracker to suggest feature requests, report bugs, and ask questions.
This is also a great way to connect with the developers of the project as well
as other interested members of the community.

Use the issue tracker to find ways to contribute. Find a bug or a feature, mention in
the issue that you will take on that effort, then follow the _Changing the code-base_
guidance below.


## Changing the code-base

Generally speaking, to make code changes you should fork this repository, make a
new branch, make changes in your own fork/branch, and then
submit a pull request to incorporate changes into the main
codebase.
See the document on [Git usage](doc/GIT_USAGE.md) for more details.

## Style, testing, and demonstration
[//]: # (TODO: add section/document on code style)

All new code *should* have associated
unit tests that validate implemented features and the presence
or lack of defects. If possible, the submitted code
should have some kind of demonstration notebook or CLI script
to go with it so that we can help others experiment with our project.

Additionally, contributed code should follow general best practices for style and
architecture.
In particular, we request for python code that you use ['Black'](https://pypi.org/project/black/)
before submitting any pull requests.
