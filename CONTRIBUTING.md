# Contributing to `rustvello`

Contributions are welcome, and they are greatly appreciated! Every little bit helps, and credit will always be given.

You can contribute in many ways:

## Types of Contributions

### Report Bugs

Report bugs at <https://github.com/pynenc/rustvello/issues>

If you are reporting a bug, please include:

- Your operating system name and version.
- Any details about your local setup that might be helpful in troubleshooting.
- Detailed steps to reproduce the bug.

### Fix Bugs

Look through the GitHub issues for bugs.
Anything tagged with "bug" and "help wanted" is open to whoever wants to implement a fix for it.

### Implement Features

Look through the GitHub issues for features.
Anything tagged with "enhancement" and "help wanted" is open to whoever wants to implement it.

### Write Documentation

`rustvello` could always use more documentation, whether as part of the official docs or in docstrings.

### Submit Feedback

The best way to send feedback is to file an issue at <https://github.com/pynenc/rustvello/issues>.

If you are proposing a new feature:

- Explain in detail how it would work.
- Keep the scope as narrow as possible to make it easier to implement.

---

## Labels

We use a set of standardized labels to categorize issues and pull requests. Proper labeling helps in tracking progress, prioritizing tasks, and organizing contributions effectively.

### **Available Labels**

#### **SemVer Labels**

- **major**
  _Description:_ Introduces incompatible API changes
  _Color:_ `d73a4a`

- **minor**
  _Description:_ Adds functionality in a backwards-compatible manner
  _Color:_ `a2eeef`

- **patch**
  _Description:_ Backwards-compatible bug fixes
  _Color:_ `00ce44`

#### **Conventional Commit Labels**

- **feature**
  _Description:_ New features
  _Color:_ `a2eeef`

- **fix**
  _Description:_ Bug fixes
  _Color:_ `d73a4a`

- **docs**
  _Description:_ Documentation updates
  _Color:_ `0075ca`
  _Patterns:_

  - `docs/**`
  - `*.md`

- **chore**
  _Description:_ Maintenance tasks
  _Color:_ `999999`

- **refactor**
  _Description:_ Code changes that neither fix a bug nor add a feature
  _Color:_ `d4c5f9`

- **security**
  _Description:_ Security-related changes
  _Color:_ `e4e669`

- **dependencies**
  _Description:_ Dependency updates
  _Color:_ `00ce44`

#### **Additional Essential Labels**

- **breaking**
  _Description:_ Breaking Changes
  _Color:_ `7c3ea5`

- **deprecated**
  _Description:_ Deprecations
  _Color:_ `f00000`

- **removed**
  _Description:_ Removals
  _Color:_ `f00000`

- **testing**
  _Description:_ Adding missing tests or correcting existing tests
  _Color:_ `79e57f`

- **skip-changelog**
  _Description:_ Changes that should be omitted from the release notes
  _Color:_ `ededed`

#### **GitHub Default Labels**

- **good first issue**
  _Description:_ Good for newcomers
  _Color:_ `7057ff`

- **help wanted**
  _Description:_ Extra attention is needed
  _Color:_ `008672`

### **Using Labels**

- **Issues:** When reporting or addressing an issue, apply the relevant labels to categorize the type of issue (e.g., bug, feature request).
- **Pull Requests:** Ensure your PRs are labeled appropriately based on the nature of the changes (e.g., fix, feature).

### **Syncing Labels**

Our workflow automatically syncs labels from the `.github/labels.yml` configuration. If you need to add or modify labels, please update the `labels.yml` file accordingly and submit a PR.

---

## Commit and PR Message Guidelines

To maintain a clean and understandable project history, we adhere to the **Conventional Commit** format for all commit messages and PR titles. This standardization facilitates automated tooling and ensures clarity in the purpose of changes.

### **Conventional Commit Format**

Each commit message should follow this structure:

```plaintext
<type>(<scope>): <description>

[optional body]

[optional footer(s)]
```

#### **Conventional Commit Format Components:**

- **`<type>`**: Describes the category of the commit. Must be one of the following:

  - `feat`: A new feature.
  - `fix`: A bug fix.
  - `docs`: Documentation only changes.
  - `style`: Changes that do not affect the meaning of the code (white-space, formatting, missing semi-colons, etc.).
  - `refactor`: A code change that neither fixes a bug nor adds a feature.
  - `perf`: A code change that improves performance.
  - `test`: Adding missing tests or correcting existing tests.
  - `build`: Changes that affect the build system or external dependencies.
  - `ci`: Changes to our CI configuration files and scripts.
  - `chore`: Other changes that don't modify src or test files.
  - `revert`: Reverts a previous commit.

- **`<scope>`**: _(Optional)_ A noun describing a section of the codebase enclosed in parentheses. For example, `feat(auth): add login functionality`.

- **`<description>`**: A short, imperative description of the change.

#### **Conventional Commit Format Examples:**

- **Feature Addition:**

  ```plaintext
  feat(auth): add login functionality
  ```

- **Bug Fix:**

  ```plaintext
  fix(api): handle null responses gracefully
  ```

- **Documentation Update:**

  ```plaintext
  docs(readme): update installation instructions
  ```

- **Refactoring Code:**

  ```plaintext
  refactor(utils): simplify error handling
  ```

- **Chore:**

  ```plaintext
  chore(deps): update dependency lodash to v4.17.21
  ```

### **Pull Request Titles**

Your PR title should mirror your commit message format for consistency.

#### **Pull Request Titles Format:**

```plaintext
<type>(<scope>): <description>
```

#### **Pull Request Titles Examples:**

- **Feature PR:**

  ```plaintext
  feat(auth): implement OAuth2 authentication
  ```

- **Bug Fix PR:**

  ```plaintext
  fix(ui): resolve button alignment issue on mobile
  ```

- **Documentation PR:**

  ```plaintext
  docs(api): add examples for new endpoints
  ```

### **Guidelines for Writing Commit Messages and PR Titles**

1. **Be Clear and Concise:**
   Ensure that your descriptions accurately reflect the changes made.

2. **Use the Imperative Mood:**
   Start your descriptions with a verb (e.g., "Add", "Fix", "Update").

3. **Reference Issues and PRs:**
   If your commit or PR relates to an existing issue, reference it in the footer using `Closes #<issue-number>` or `Fixes #<issue-number>`.

4. **Avoid Subjective Language:**
   Stick to objective descriptions that describe the "what" and "why" of the changes.

---

## Get Started

Ready to contribute? Here's how to set up `rustvello` for local development. Please note this documentation assumes you already have `uv`, `make`, and `Git` installed and ready to go.

1. **Fork the `rustvello` repo on GitHub.**

2. **Clone your fork locally:**

   ```bash
   cd <directory_in_which_repo_should_be_created>
   git clone git@github.com:YOUR_NAME/rustvello.git
   ```

3. **Navigate into the directory:**

   ```bash
   cd rustvello
   ```

4. **Install dependencies and pre-commit hooks using `make`:**

   ```bash
   make install
   ```

5. **Create a branch for local development:**

   ```bash
   git checkout -b name-of-your-bugfix-or-feature
   ```

   Now you can make your changes locally.

6. **Testing `ClientType`:**
   By default, the tests will run using the `Mock` client. If you want to test with the `Poet` client, which requires a running Poet instance, you need to set the `rustvello_CLIENT_TYPE` environment variable to `Poet`.

   **Running tests with `Mock` (default):**

   ```bash
   make test
   ```

   **Running integration tests with `Poet`:**

   ```bash
   export rustvello_CLIENT_TYPE=Poet
   make test
   ```

   or

   ```bash
   make test-integration
   ```

7. **Add test cases for your changes:**
   Add all test cases to the `tests` directory.

8. **Run formatting and linting checks:**

   ```bash
   make check
   ```

9. **Validate all unit tests pass:**

   ```bash
   make test
   ```

10. **Run `tox` to test across multiple Python versions:**

    ```bash
    tox
    ```

    _This step requires multiple Python versions installed. It is optional as it runs automatically in the CI/CD pipeline._

11. **Commit your changes and push your branch to GitHub:**

    ```bash
    git add .
    git commit -m "Your detailed description of your changes."
    git push origin name-of-your-bugfix-or-feature
    ```

12. **Submit a pull request on GitHub.**

---

## Pull Request Guidelines

Before you submit a pull request, ensure it meets the following guidelines:

1. **Adhere to Conventional Commit Standards:**
   Ensure your commit messages and PR titles follow the **Conventional Commit** format as outlined above.

2. **Include Tests:**
   Your pull request should include tests that cover the changes made. This ensures that new features work as expected and that bugs are properly fixed.

3. **Update Documentation:**
   If your pull request adds functionality, ensure that the documentation is updated accordingly. This includes:

   - Adding docstrings for new functions or modules.
   - Updating the feature list in the `README.md` if applicable.

4. **Run CI/CD Checks:**
   Ensure all automated tests and checks pass before submitting your PR. Address any issues that arise during these checks.

5. **Link to Relevant Issues:**
   If your PR addresses an existing issue, reference it in the PR description using `Closes #<issue-number>` or `Fixes #<issue-number>`.

6. **Provide a Clear Description:**
   In your PR description, provide a clear and detailed explanation of what your changes do and why they are necessary. Include any relevant context or screenshots if applicable.

7. **Ensure Proper Labeling:**
   Based on your contributions and the nature of your changes, ensure that your PR is labeled appropriately using the standardized labels mentioned above.

---

## Labels and Automation

Our project utilizes automated workflows to manage labels and releases. Understanding how these systems work will help you contribute more effectively.

### **Label Management**

We use GitHub Actions to automatically sync and manage labels based on our `.github/labels.yml` configuration. This ensures consistency across all issues and pull requests.

- **Syncing Labels:**
  The `crazy-max/ghaction-github-labeler@v5` action syncs labels defined in `.github/labels.yml` with your GitHub repository. Ensure that any new labels you introduce are added to this configuration file.

- **Applying Labels Based on PR Titles:**
  Pull requests are automatically labeled based on their titles using the `bcoe/conventional-release-labels@v1` action. Ensure your PR titles follow the **Conventional Commit** format to benefit from this automation.

### **Release Management**

We use **Release Drafter** to automate the creation of release notes based on merged pull requests.

- **Release Drafter Configuration:**
  The configuration file `.github/release-drafter.yml` defines how release notes are generated, categorizing changes based on labels.

- **Automated Releases:**
  When changes are pushed to the `main` branch, the `release-drafter/release-drafter@v5` action generates or updates a draft release, aggregating changes from merged PRs.

- **Publishing Packages:**
  Our CI/CD pipeline handles publishing Rust crates and Python packages. It includes checks to prevent publishing duplicate versions and ensures that releases are only created when new versions are available.

---

## Handling Labeling and Release Errors

### **Release Drafter Errors**

If you encounter errors related to Release Drafter, such as missing configuration files or invalid templates:

1. **Ensure Configuration File Exists:**
   Verify that `.github/release-drafter.yml` exists in the default branch and is correctly formatted.

2. **Validate YAML Syntax:**
   Use a YAML linter to ensure there are no syntax errors in your configuration.

3. **Use Supported Placeholders:**
   Ensure your `template` uses valid placeholders like `$CHANGES`, `$CATEGORIES`, and `$CONTRIBUTORS`.

### **Sync Labels Errors**

If syncing labels fails due to incorrect color codes or invalid configurations:

1. **Quote Color Codes:**
   Ensure all color codes in `.github/labels.yml` are quoted strings, especially if they are purely numeric.

   ```yaml
   - name: chore
     description: Maintenance tasks
     color: "999999"
   ```

2. **Include All Managed Labels:**
   Add commonly used labels like `good first issue` and `help wanted` to your `labels.yml` to prevent exclusion warnings.

   ```yaml
   - name: good first issue
     description: Good for newcomers
     color: "7057ff"
   - name: help wanted
     description: Extra attention is needed
     color: "008672"
   ```

3. **Review Workflow Permissions:**
   Ensure your GitHub Actions workflows have the necessary permissions to manage labels and issues.

---

## Best Practices

- **Consistent Labeling:**
  Always use the predefined labels to maintain consistency across the project.

- **Clear Commit Messages:**
  Follow the **Conventional Commit** format to make your commit history clear and useful.

- **Comprehensive Testing:**
  Include tests with your contributions to ensure that new features work as intended and that existing functionality is not broken.

- **Detailed Documentation:**
  Update documentation alongside your code changes to help other contributors understand and use new features.

---

Thank you for contributing to `rustvello`! Your efforts help make this project better for everyone.

---
