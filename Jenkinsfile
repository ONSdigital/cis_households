#!groovy
// Checkout the Jenkinsfile docs at https://jenkins.io/doc/book/pipeline/jenkinsfile/
// ONS docs on Jenkins are at https://collaborate2.ons.gov.uk/confluence/pages/viewpage.action?spaceKey=REG&title=Jenkins

// Global scope required for multi-stage persistence
def artServer = Artifactory.server "art-p-01"
def buildInfo = Artifactory.newBuildInfo()
def agentPython3Version = 'python_3.6.1'

def pushToPyPiArtifactoryRepo(String projectName, String sourceDist = 'dist/*', String artifactoryHost = 'art-p-01') {
    withCredentials([
        usernamePassword(credentialsId: env.ARTIFACTORY_CREDS, usernameVariable: 'ARTIFACTORY_USER', passwordVariable: 'ARTIFACTORY_PASSWORD'),
        string(credentialsId: env.ARTIFACTORY_PYPI_REPO, variable: 'ARTIFACTORY_REPO')
        ]) {
            sh "curl -u ${ARTIFACTORY_USER}:\${ARTIFACTORY_PASSWORD} -T ${sourceDist} 'http://${artifactoryHost}/artifactory/${ARTIFACTORY_REPO}/${projectName}/'"
    }
}

pipeline {
    // Define env variables
    environment {
        PROJECT_NAME = 'cishouseholds'
        ARTIFACTORY_CREDS = 'ARTIFACTORY_CREDS' // set in Jenkins Credentials
        ARTIFACTORY_PYPI_REPO = 'ARTIFACTORY_REPO' // set in Jenkins Credentials
    }

    // Don't use default checkout process, as we define it as a stage below
    options {
        skipDefaultCheckout true
    }

    // Driver Agent must always be set at the top level of the pipeline
    // We're not picky
    agent any
    // Keep getting intermittent network errors, so retry each stage
    stages {
            // Checkout stage to fetch code from  GitLab
        stage("Checkout") {
            // We have to specify an appropriate slave for each stage
            // Choose from download, build, test, deploy
            agent { label "download.jenkins.slave" }
            steps {
                sh "echo 'Checking out code from source control.'"
                retry(3) {
                    checkout scm
                    // Stash the files that have been checked out, for use in subsequent stages
                    stash name: "Checkout", useDefaultExcludes: false
                }
            }

        }
        stage("Build") {
            agent { label "build.${agentPython3Version}" }
            steps {
                unstash name: 'Checkout'
                sh "echo 'Building package.'"
                retry(3) {
                    sh 'pip3 install wheel==0.29.0'  // Later versions not compatible with Python 3.6
                    sh 'python3 setup.py build bdist_wheel'
                    stash name: "Build", useDefaultExcludes: false
                }
            }
        }
        stage("Deploy") {
            when { tag "v*" }
            agent { label "test.${agentPython3Version}" } // Deploy agent didn't seem to be able to push
            steps {
                unstash name: "Build"
                sh "echo 'Deploying package to Artifactory'"
                retry(3) {
                    pushToPyPiArtifactoryRepo(PROJECT_NAME)
                }
            }
        }
    }
}
