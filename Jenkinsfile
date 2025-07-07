#!groovy
def BN = (BRANCH_NAME == 'master' || BRANCH_NAME.startsWith('releases/')) ? BRANCH_NAME : 'releases/2025-12'

library "knime-pipeline@$BN"

properties([
    pipelineTriggers([
        upstream('knime-base/' + env.BRANCH_NAME.replaceAll('/', '%2F')),
    ]),
	parameters(workflowTests.getConfigurationsAsParameters()),
    buildDiscarder(logRotator(numToKeepStr: '5')),
    disableConcurrentBuilds()
])

try {
    // provide the name of the update site project
    knimetools.defaultTychoBuild('org.knime.update.powerbi')

    workflowTests.runTests(
        dependencies: [
            repositories:  [
                'knime-aws',
                'knime-azure',
                'knime-bigdata',
                'knime-bigdata-externals',
                'knime-cloud',
                'knime-credentials-base',
                'knime-database',
                'knime-database-proprietary',
                'knime-filehandling',
                'knime-gateway',
                'knime-js-base',
                'knime-kerberos',
                'knime-office365',
                'knime-powerbi'
                ],
            ius: [
                'org.knime.features.database.extensions.sqlserver.driver.feature.group'
            ]
        ]
    )

    stage('Sonarqube analysis') {
        env.lastStage = env.STAGE_NAME
		// TODO remove empty list once workflow tests are enabled
        workflowTests.runSonar([])
    }
} catch (ex) {
    currentBuild.result = 'FAILURE'
    throw ex
} finally {
    notifications.notifyBuild(currentBuild.result);
}
/* vim: set shiftwidth=4 expandtab smarttab: */
