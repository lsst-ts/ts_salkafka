pipeline {
    agent any

    stages {
        stage("Running tests") {
            steps {
                script {
                    sh """
                    docker pull lsstts/salobj:master
                    chmod -R a+rw \${WORKSPACE}
                    container=\$(docker run -v \${WORKSPACE}:/home/saluser/repo/ -td --rm lsstts/salobj:master)
                    docker exec -u saluser \${container} sh -c \"source ~/.setup.sh && cd repo && eups declare -r . -t saluser && setup ts_salkafka -t saluser && scons\"
                    docker stop \${container}
                    """
                }
            }
        }
    }
    post {
        always {
            // The path of xml needed by JUnit is relative to
            // the workspace.
            junit 'tests/.tests/*.xml'

            // Publish the HTML report
            publishHTML (target: [
                allowMissing: false,
                alwaysLinkToLastBuild: false,
                keepAll: true,
                reportDir: 'tests/.tests/pytest-ts_salkafka.xml-htmlcov/',
                reportFiles: 'index.html',
                reportName: "Coverage Report"
              ])
        }
        cleanup {
            sh """
                container=\$(docker run -v \${WORKSPACE}:/home/saluser/repo/ -td --rm lsstts/salobj:master)
                docker exec -u root --privileged \${container} sh -c \"chmod -R a+rw /home/saluser/repo/ \"
                docker stop \${container}
            """
            deleteDir()
        }
    }
}
