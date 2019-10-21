pipeline {
    agent any

    stages {

        stage('Compile') {
            steps {
                echo "Compiling..."
                sh 'mvn -B -DskipTests clean package'
            }
        }
    }
}