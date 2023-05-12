<!--- TODO: Consider using or merging this GitHub description with https://github.com/NOAA-OWP/DMOD/blob/master/doc/GIT_USAGE.md --->
## The GitHub-based contribution process in summary, i.e., TL;DR
* Fork the data_access_examples repo on GitHub and clone the new fork  to your development environment

```
git clone github.com/<githubusername>/<repository_name>
```

* Make a new branch and make your changes to the source files in the branch

* Add changed files to the commit ... `git add <list of your changed files>`

* ... and commit them: `git commit`

* Push the accumulated commits to your fork/branch in GitHub: `git push`

* Open GitHub and issue a pull request.

* **IMPORTANT** Keep your main branch code up-to-date with the upstream repository using the following commands. As commits from other developers accumulate in the main, merge these into your fork prior to issuing a pull request.

```
git remote add upstream https://github.com/alabamawaterinstitute/data_access_examples.git
git fetch upstream  # updates local repository with upstream commits by others
git pull --rebase upstream main  # rebases your commits on top of upstream commits

```
## A more detailed step-by-step guide to contributing via GitHub

### 1. On GitHub, create a fork and clone it to your local directory
A step-by-step guide illustrating how to fork and clone
a repository can be found
[here](https://help.github.com/en/github/getting-started-with-github/fork-a-repo).
Once a new fork is created, clone it to an empty local directory
of your choice (if the directory does not exist already,
`git clone` will create it):

```
# navigate to a local directory where you want to store the repo
cd <local/directory/of/your/choice>

# clone your forked repo
git clone github.com/<githubusername>/data_access_examples.git
 ```
____

### 2. Set up the Git configuration parameters

Set up your user name and email in your local repo config

```
git config user.name "Your name as you would like it displayed in the commit"
git config user.email "Your email address"
```
Alternatively, you can change your machine global git configs:

```
git config --global user.name "Your name as you would like it displayed in the commit"
git config --global user.email "Your email address"
```
____

<!--- Please check if this is the default behavior. If so,
we can probably remove this step --->
### 3. Make your fork a remote
Your GitHub fork that you just cloned from will be labeled as the `origin`
remote in your local git repository. If for some reason you are working from another
fork, you can add yours like this:
```
git remote add origin main https://github.com/<githubusername>/data_access_examples.git
```
____

### 4. Add the upstream data_access_examples repo as a second remote

To get the latest updates from the main data_access_examples codebase,
we need to add it as a second remote. It is common to
name this remote, `upstream`

```
git remote add upstream https://github.com/alabamawaterinstitute/data_access_examples.git
```

____

### 5. Set up proper push URL for your remote

Later in the process, you will need to push commits back to your fork. Because GitHub
imposes security limitations on pushing via https, you will either need to
configure a personal token or add an ssh token and change the push url for your remote.
This isn't necessary until you make a change and try to push it, but while you are
adding the remote, it is convenient to set up.

In order to change the push URL of a Git remote,
you can follow the steps below.

  1.	Change to the directory where the repository is located:

      ```
		cd /path/to/repository
	  ```
  2.	Run git remote to list the existing remotes and see their names and URLs:
`git remote -v`

The output will look something like this:

```
		origin	https://github.com/<githubusername>/repo_name.git (fetch)
		origin	https://github.com/<githubusername>/repo_name.git (push)
```
  3.	Change your remote's push URL from HTTPS to SSH with the git remote set-url
command. Use the git remote set-url command followed by the remote name, and the remote’s URL:
`git remote set-url --push <remote-name> <remote-url>`

To change the URL of the origin to git@github.com:user/repo_name.git you would type:

```
		git remote set-url --push origin git@github.com:<githubusername>/data_access_examples.git

```
  4.	Now Verify that the remote URL has changed with `git remote -v`

  5. Generate a new SSH key, following instructions [here](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent):

```
	    ssh-keygen -t ed25519 -C your_email@example.com
		
```

When you're prompted to "Enter a file in which to save the key", you can press Enter to accept the default file location.

  3.	Print out the contents of the public key and copy these for paasing into the GitHub account.

```
		cat <location where the public key was saved in step 2, probably "id_ed25519.pub">
```
After getting the SSH key, save it in your GitHub account per instructions [here](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account)

____

### 8. Incorporate upstream changes into your fork

To incorporate any changes from the main codebase into your
local repo main, `checkout` your `main` branch and do a
fetch-and-rebase using the `pull` command.

```
# checkout the main branch
git checkout main

# fetch and rebase changes
git pull --rebase upstream main
```

Finally, use the `push` command so these updates are in the
GitHub main branch on your fork.

```
# assuming you still have `main` checked out
git push
```

____


### 9. Create a new development branch

Branches are parallel versions of the repository that allow you
to make changes and tinker as much as you would like without
disrupting the main codebase. It is recommended to create a
new branch for each bite-sized contribution you make or issue
you respond to.

The above fetch-and-rebase-into-main process is superfluous in some ways.
By maintaining a connection to the upstream repository, you can
always have access to the latest version of the upstream codebase.

If you always make your development branches start from the upstream,
it will help make your commits much easier to rebase and merge into
the main development branch when you are ready to contribute them.

Here is how to create a new branch:

```
git fetch upstream
git checkout upstream/main
```

This will update your local copy of upstream and checkout a
'detached HEAD' version of it. (There will be a message that
explains what that means when you do it.)

Then create your development branch from the 'detached HEAD' branch:
```
git checkout -b <new-branch-name>
```

The above command will create *and* checkout the new branch.
Otherwise, the `checkout` command is used to navigate between
branches. It updates files in your working directory to match
the version stored in that branch.

```
git checkout <branch-name>
```
____

### 6. Push the local branch to the remote repository

At anytime after you create your development branch, you can synchronize your local commits
(there will be nothing to start with; much more later) with you GitHub fork.

To Push the local branch to the remote repository,

```
		git push -u origin <branch-name>
		or
		git push –set-upstream origin <branch-name>

```
____


### 10. Incorporate upstream changes into your development branch

Any time you update `main` with changes from the `upstream`
remote (step 5), it is advised to rebase any local development
branches, too. Rebasing ensures that your changes are based
from what everyone else has already done.

```
# check out the branch you would like to rebase
git checkout <branch-name>

# rebase the branch
git rebase main
```
____

### 11. Tinker in your development branches.
In your local development branches, explore the code, make changes,
and tinker at will. **Be creative!**

____

### 12. Add new and changed files to the staging area

```
git add <list of your changed files>
```
____

### 13. Commit your changes to the checked out development branch
Commits should be accompanied by a concise and brief comment.
Like the "save" button in a word processor, commits should be made
early and often.
A small commit with a precise comment, is often
more useful that a massive commit with a detailed changelist buried in bullets.
If additional comments are necessary, these may be included following a
blank line as shown below.

```
git commit -m 'Make Brief comments to explain your Feature

Additional detail below a blank line, like this.
-An explanation of why a change was made can help here
-Bullets are useful for a large commit
-But usually, it is better to just commit more often
-With a short one-line description of each little change'
```
____

### 14. Push commits to GitHub
In order to get any accumulated commits in GitHub on your remote branch,
push the changes. Make sure that you have the correct development branch
checked out, first.

```
# make sure the right development branch is checked out. There will be "*" next to the checked out branch
git branch -vv

# if the correct development branch is not checked out, change it
git checkout <branch-name>

# push commits to the development branch
git push origin <branch-name>
```
____

### 15. Submit a pull request
After you git push all commits to your branch and you believe
you are ready to post code to the main codebase, open GitHub
and issue a pull request. (It will probably be a highlighted
button at the top of the page -- "New Pull Request"). Of course,
please test, document, style check
(see ['Black'](https://pypi.org/project/black/)),
and prepare (as-needed) demonstration notebooks, etc.
More information about pull requests can be found
[here](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/about-pull-requests).
From that page:
> "Pull requests let you tell others about changes you've pushed
to a branch in a repository on GitHub. Once a pull request is
opened, you can discuss and review the potential changes with
collaborators and add follow-up commits before your changes
are merged into the base branch."

A pull request will allow someone else to look at your code with
you to make sure that it is ready to share with the world.
Most of the time, someone who was involved with preparing
the code can be the reviewer; for major changes, it should
be someone outside the core development team.
____

### 16. Useful references
- If some of the terminology used above is confusing - we agree.
This [GitHub glossary](https://help.github.com/en/github/getting-started-with-github/github-glossary#checkout)
is a useful reference.
-  For a better guide on how to use GitHub to collaborate on
NOAA development projects, please see the
[documentation associated with the DMOD repo](https://github.com/NOAA-OWP/DMOD/blob/master/doc/GIT_USAGE.md#contributing-tldr)

