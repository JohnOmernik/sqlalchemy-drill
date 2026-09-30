// Internal publication path for sqlalchemy-drill.
//
// Publishes to the Preset-internal package index, which is backed by the
// s3://preset-pypi bucket.  Every operation in this file talks to the bucket
// over the AWS API using the runtime-bound 'ci-user' credential; the index's
// public hostname is deliberately not referenced here.  Consumers resolve the
// artifact through the documented internal index base URL, which is held in
// internal documentation rather than in this repository.
//
// Modelled on the existing Preset publishers (claude-commands, service-lib-py,
// api-clients).  No credential material lives in this file: the AWS keys are
// bound at runtime by Jenkins and are never echoed.
//
// WHAT IS PUBLISHED
// -----------------
// The pure-Python wheel ONLY.  The sdist is still built, because
// tools/check_dist.py validates the wheel and the sdist as a pair and we want
// that gate to keep running in full -- but the sdist is never uploaded.  The
// reason is reproducibility: the wheel is byte-identical across rebuilds when
// SOURCE_DATE_EPOCH is pinned, whereas the sdist is not reproducible even with
// it (setuptools bakes in member ordering and mtimes).  Only the wheel can be
// independently rebuilt and checked against what is stored, so only the wheel
// is fit to be pinned by a consumer.
//
// VERSIONING
// ----------
// Four-component convention already used in this bucket (upstream PyHive
// 0.7.0 -> Preset 0.7.0.1 / 0.7.0.2).  The Preset build of upstream 1.1.11 is
// 1.1.11.1, declared in sqlalchemy_drill/__init__.py so the published version
// is auditable in git rather than synthesised here.
//
//   * master      -> stable 1.1.11.1.  Published ONLY from reviewed, merged
//                    Preset history.
//   * PR branches -> 1.1.11.1+pr.<n>.<shortsha>, a PEP 440 local version that
//                    is deliberately NOT a stable release.  A PR build can
//                    never emit the bare stable version; this is asserted
//                    below rather than left to convention.
//   * other branches (the branch job that runs next to a PR's pr-head job)
//                 -> 1.1.11.1+branch.<shortsha>; tests and the wheel build
//                    run, nothing is published.
//
// 1.1.11.1 sorts ABOVE a hypothetical upstream 1.1.11, so it must never be
// offered to a dependency resolver as a candidate for the plain
// `sqlalchemy-drill` name.  Consumers pin the immutable wheel URL directly.
//
// IMMUTABILITY
// ------------
// Published artifacts are never overwritten.  `aws s3 sync` is deliberately
// NOT used, because it replaces an object whose content has changed.  Upload
// goes through `aws s3api put-object --if-none-match '*'`, which S3 rejects
// with a 412 if the key already exists, making the no-overwrite guarantee
// atomic rather than a check-then-write race.
//
// PREREQUISITE FOR THE JENKINS ADMIN: `--if-none-match` needs AWS CLI v2.18 or
// newer in the 'ci' image.  If the image is older the upload step fails loudly
// and nothing is published -- that is intended (fail closed).  Do not "fix" it
// by falling back to `aws s3 sync`, which would silently permit an overwrite.

// Bucket prefix / requirement name.  These differ from the artifact filename:
// setuptools emits the underscored distribution name, while the bucket prefix
// and the Shell requirement use the dashed project name.  Conflating the two
// makes the existence check probe a key that never exists, so it always
// reports "absent" and an overwrite slips through silently.
LIB_NAME = 'sqlalchemy-drill'
DIST_NAME = 'sqlalchemy_drill'
BUCKET = 'preset-pypi'

String baseVersion = ""
String publishVersion = ""
String wheelName = ""

podTemplate(
    imagePullSecrets: ['preset-pull'],
    nodeUsageMode: 'NORMAL',
    containers: [
        containerTemplate(
            alwaysPullImage: true,
            name: 'ci',
            image: 'preset/ci:latest',
            ttyEnabled: true,
            command: 'cat',
            resourceRequestCpu: '100m',
            resourceLimitCpu: '200m',
            resourceRequestMemory: '1000Mi',
            resourceLimitMemory: '2000Mi',
        ),
        containerTemplate(
            alwaysPullImage: true,
            name: 'py-ci',
            image: 'preset/python:3.9.18-2024-02-21-ci',
            ttyEnabled: true,
            command: 'cat'
        )
    ]
) {
    node(POD_LABEL) {
        def repo = checkout scm
        def shortGitRev = sh(
                returnStdout: true,
                script: 'git rev-parse --short HEAD'
        ).trim()

        boolean isMaster = (env.BRANCH_NAME == 'master')
        boolean isPullRequest = env.BRANCH_NAME.startsWith('PR-')
        boolean publishes = isMaster || isPullRequest

        container('py-ci') {
            stage('Resolve version') {
                // The package is the single source of truth.  Read it without
                // importing so a broken build cannot fake the version.
                baseVersion = sh(
                        script: "grep '^__version__' sqlalchemy_drill/__init__.py | head -1 | cut -d\"'\" -f2",
                        returnStdout: true,
                        label: 'Read declared version'
                ).trim()

                if (isPullRequest) {
                    publishVersion = "${baseVersion}+${env.BRANCH_NAME}.${shortGitRev}"
                } else if (isMaster) {
                    publishVersion = baseVersion
                } else {
                    // Plain branch builds verify but never publish.
                    publishVersion = "${baseVersion}+branch.${shortGitRev}"
                }

                // A stable release may only come from reviewed, merged history.
                if (!isMaster && publishVersion == baseVersion) {
                    error("Refusing to build stable version ${baseVersion} from branch " +
                          "'${env.BRANCH_NAME}'. Stable releases are published only from master.")
                }

                // Use the artifact checker's normalization, not a second set
                // of PEP 440 rules in Groovy. Normalize before stamping the
                // package or deriving any artifact filename or upload key.
                // Keep the stable-version guard above this conversion so a
                // spelling-only change cannot make a branch look non-stable.
                sh(script: 'python -m pip install packaging', label: 'Install version tooling')
                publishVersion = sh(
                        script: "python tools/check_dist.py --print-normalized '${publishVersion}'",
                        returnStdout: true,
                        label: 'Normalize publish version'
                ).trim()
                if (!isMaster && !publishVersion.contains('+')) {
                    error("Non-master build produced a non-local version ${publishVersion}; refusing.")
                }

                wheelName = "${DIST_NAME}-${publishVersion}-py3-none-any.whl"
                echo "Base version: ${baseVersion}"
                echo "Publish version: ${publishVersion}${isMaster ? ' (STABLE)' : ' (pre-release)'}"
            }

            stage('Tests') {
                sh(script: 'python -m pip install --upgrade pip', label: 'Upgrade pip')
                sh(script: 'pip install -r requirements/test.txt', label: 'Install test dependencies')
                // Compilation/reflection regressions plus the artifact gate.
                // These need no Drill server, so they run on every build.
                sh(
                    script: 'python -m pytest -v test/test_sqlalchemy2_reflection.py tools/test_check_dist.py',
                    label: 'Unit and artifact-gate tests'
                )
            }
        }

        container('ci') {
            stage('Reject an already-published version') {
                if (!publishes) {
                    echo "Branch '${env.BRANCH_NAME}' does not publish. Skipping."
                    return
                }
                withCredentials([
                    [
                        $class           : 'AmazonWebServicesCredentialsBinding',
                        credentialsId    : 'ci-user',
                        accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                        secretKeyVariable: 'AWS_SECRET_ACCESS_KEY',
                    ]
                ]) {
                    // Queried against the bucket over the AWS API rather than
                    // over the index's public HTTP front door: it is the
                    // authoritative source, it needs no hostname, and it is not
                    // subject to any caching in front of the index.
                    def exists = sh(
                            script: "aws s3api head-object --bucket ${BUCKET} --key ${LIB_NAME}/${wheelName} >/dev/null 2>&1",
                            returnStatus: true,
                            label: 'Check for an existing wheel'
                    )
                    if (exists == 0) {
                        error("${wheelName} is already published. Published artifacts are " +
                              "immutable; bump the version in sqlalchemy_drill/__init__.py.")
                    }
                }
            }
        }

        container('py-ci') {
            stage('Build and verify') {
                if (!isMaster) {
                    sh(
                        script: "sed -i \"s/__version__ = '${baseVersion}'/__version__ = '${publishVersion}'/\" sqlalchemy_drill/__init__.py",
                        label: 'Apply pre-release version'
                    )
                }

                // Pin the build clock to the commit so the wheel is
                // byte-reproducible from this exact revision.
                sh(script: 'python -m pip install build twine', label: 'Install build tooling')
                sh(
                    script: '''
                        set -eu
                        SOURCE_DATE_EPOCH="$(git -c safe.directory="$PWD" log -1 --pretty=%ct)"
                        case "$SOURCE_DATE_EPOCH" in
                            ''|*[!0-9]*) echo "invalid SOURCE_DATE_EPOCH: $SOURCE_DATE_EPOCH" >&2; exit 1 ;;
                        esac
                        export SOURCE_DATE_EPOCH
                        python -m build
                    ''',
                    label: 'Build wheel and sdist'
                )

                // Both artifacts are built so this gate can run in full: it
                // checks the pair, rejects a version mismatch, and rejects a
                // wheel that ships the top-level test package.  Only the wheel
                // is uploaded afterwards.
                sh(
                    script: "python tools/check_dist.py dist --expected-version '${publishVersion}'",
                    label: 'Verify artifact contents'
                )
                sh(script: 'python -m twine check --strict dist/*', label: 'Verify artifact metadata')

                // Stage the wheel alone.  The sdist is intentionally left
                // behind and never reaches the bucket.
                sh(
                    script: "mkdir -p upload/${LIB_NAME} && cp dist/${wheelName} upload/${LIB_NAME}/",
                    label: 'Stage wheel for upload'
                )
                sh(
                    script: "sha256sum upload/${LIB_NAME}/${wheelName} | cut -d' ' -f1 > wheel.sha256",
                    label: 'Local wheel digest'
                )
            }
        }

        container('ci') {
            stage('Publish wheel') {
                if (!publishes) { return }
                withCredentials([
                    [
                        $class           : 'AmazonWebServicesCredentialsBinding',
                        credentialsId    : 'ci-user',
                        accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                        secretKeyVariable: 'AWS_SECRET_ACCESS_KEY',
                    ]
                ]) {
                    // Jenkins has AWS CLI v1, which cannot express If-None-Match.
                    // A current boto3 client preserves the same atomic 412 guard.
                    sh(
                        script: """
                            set -eu
                            python -m pip install --quiet 'boto3>=1.36,<2'
                            BUCKET='${BUCKET}' KEY='${LIB_NAME}/${wheelName}' ARTIFACT='upload/${LIB_NAME}/${wheelName}' \
                              python -c 'import os, boto3; artifact = open(os.environ["ARTIFACT"], "rb"); boto3.client("s3").put_object(Bucket=os.environ["BUCKET"], Key=os.environ["KEY"], Body=artifact, IfNoneMatch="*")'
                        """,
                        label: 'Upload wheel (no-overwrite)'
                    )

                    // Read the stored object back and digest that, rather than
                    // trusting the local build.  This is the SHA256 a consumer
                    // pins.  It verifies the bytes at rest in the bucket; the
                    // index front door is a pass-through over the same object.
                    sh(
                        script: """
                            set -e
                            aws s3api get-object \
                              --bucket ${BUCKET} \
                              --key ${LIB_NAME}/${wheelName} \
                              stored.whl >/dev/null
                            STORED="\$(sha256sum stored.whl | cut -d' ' -f1)"
                            LOCAL="\$(cat wheel.sha256)"
                            if [ "\$STORED" != "\$LOCAL" ]; then
                                echo "Stored digest \$STORED does not match built digest \$LOCAL"
                                exit 1
                            fi
                            printf '%s  %s\\n' "\$STORED" "${wheelName}" > published.sha256
                            echo "=============================================================="
                            echo " Pin this in Shell:"
                            echo "   <internal-index-base>/${LIB_NAME}/${wheelName}"
                            echo "   sha256=\$STORED"
                            echo "=============================================================="
                        """,
                        label: 'Digest the stored artifact'
                    )
                }
                archiveArtifacts artifacts: 'published.sha256', fingerprint: true
                echo "✅ Published ${LIB_NAME} ${publishVersion}"
            }
        }
    }
}
