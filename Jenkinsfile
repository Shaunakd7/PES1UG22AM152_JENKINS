pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
                script {
                    sh 'g++ hello.cpp -o PES1UG22AM152-1'
                }
            }
        }

        stage('Test') {
            steps {
                script {
                    sh './PES1UG22AM152-1'
                }
            }
        }

        stage('Deploy') {
            steps {
                echo 'Deploying the application...'
                // Add deployment commands if needed
            }
        }
    }

    post {
        failure {
            echo 'Pipeline failed'
        }
        success {
            echo 'Pipeline executed successfully!'
        }
    }
}
