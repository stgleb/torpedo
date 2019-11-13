package tests

import (
	"fmt"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	. "github.com/portworx/torpedo/tests"
)

var (
	defaultCoolDownPeriod      = int64(60)
	pxPoolAvailableCapacity    = "100 * ( px_pool_stats_available_bytes/ px_pool_stats_total_bytes)"
	pxPoolTotalCapacity        = "px_pool_stats_total_bytes/(1024*1024*1024)"
	labelSelectorOpGt          = "Gt"
	labelSelectorOpLt          = "Lt"
	specActionName             = "openstorage.io.action.storagepool/expand"
	ruleActionsScalePercentage = "scalepercentage"
	ruleActionsMaxSize         = "maxsize"
	ruleScaleType              = "scaletype"
)

var (
	testNameSuite            = "AutopilotPoolResize"
	timeout                  = 30 * time.Minute
	scaleTimeout             = 2 * time.Hour
	workloadTimeout          = 3 * time.Hour
	retryInterval            = 30 * time.Second
	unscheduledResizeTimeout = 10 * time.Minute
)

var ruleResizeBy50IfPoolUsageMoreThan50AddDisk = scheduler.AutopilotRuleParameters{
	// Resize storage pool by 50% until the pool usage is more than 50%
	ActionsCoolDownPeriod: defaultCoolDownPeriod,
	RuleConditionExpressions: []scheduler.AutopilotRuleConditionExpressions{
		{
			Key:      pxPoolAvailableCapacity,
			Operator: labelSelectorOpLt,
			Values:   []string{"80"},
		},
	},
	RuleActions: []scheduler.AutopilotRuleActions{
		{
			Name: specActionName,
			Params: map[string]string{
				ruleActionsScalePercentage: "50",
				ruleScaleType:              "add-disk",
			},
		},
	},
	MatchLabels:      map[string]string{"name": "storage-pool-resize"},
	ExpectedPoolSize: 137438953472,
}

var ruleResizeBy50IfPoolUsageMoreThan50ResizeDisk = scheduler.AutopilotRuleParameters{
	// Resize storage pool by 50% until the pool usage is more than 50%
	ActionsCoolDownPeriod: defaultCoolDownPeriod,
	RuleConditionExpressions: []scheduler.AutopilotRuleConditionExpressions{
		{
			Key:      pxPoolAvailableCapacity,
			Operator: labelSelectorOpLt,
			Values:   []string{"90"},
		},
	},
	RuleActions: []scheduler.AutopilotRuleActions{
		{
			Name: specActionName,
			Params: map[string]string{
				ruleActionsScalePercentage: "50",
				ruleScaleType:              "resize-disk",
			},
		},
	},
	MatchLabels:      map[string]string{"name": "storage-pool-resize"},
	ExpectedPoolSize: 103079215104,
}

var autopilotruleBasicTestCases = []scheduler.AutopilotRuleParameters{
	ruleResizeBy50IfPoolUsageMoreThan50AddDisk,
}

func TestAutoPilot(t *testing.T) {
	RegisterFailHandler(Fail)

	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_autopilot.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecsWithDefaultAndCustomReporters(t, "Torpedo : Autopilot", specReporters)
}

var _ = BeforeSuite(func() {
	InitInstance()
})

// This testsuite is used for performing basic scenarios with Autopilot rules where it
// schedules apps and wait until workload is completed on the volumes and then validates
// storage pool(s) size(s) on the nodes where volumes reside
var _ = Describe(fmt.Sprintf("{%sWaitForWorkload}", testNameSuite), func() {
	It("has to fill up the volume completely, resize the storage pool(s), validate and teardown apps", func() {
		var err error
		testName := strings.ToLower(fmt.Sprintf("%sWaitForWorkload", testNameSuite))

		for _, apRule := range autopilotruleBasicTestCases {
			var contexts []*scheduler.Context

			workerNodes := node.GetWorkerNodes()
			for _, workerNode := range workerNodes {
				AddLabelsOnNode(workerNode, apRule.MatchLabels)
			}

			apParameters := &scheduler.AutopilotParameters{
				Enabled:                 true,
				Name:                    testName,
				AutopilotRuleParameters: apRule,
			}

			Step("schedule applications", func() {
				for i := 0; i < Inst().ScaleFactor; i++ {
					taskName := fmt.Sprintf("%s-%v", fmt.Sprintf("%s-%d", testName, i), Inst().InstanceID)
					context, err := Inst().S.Schedule(taskName, scheduler.ScheduleOptions{
						AppKeys:             Inst().AppList,
						StorageProvisioner:  Inst().Provisioner,
						AutopilotParameters: apParameters,
						ScaleStorageFactor:  Inst().ScaleStorageFactor,
					})
					Expect(err).NotTo(HaveOccurred())
					Expect(context).NotTo(BeEmpty())
					contexts = append(contexts, context...)
				}
			})

			for _, ctx := range contexts {
				Step("wait until workload completes on volume", func() {
					err = Inst().S.WaitForRunning(ctx, workloadTimeout, retryInterval)
					Expect(err).NotTo(HaveOccurred())
				})
			}

			Step("validating and verifying size of storage pools", func() {
				ValidateStoragePoolSize(contexts, apRule.ExpectedPoolSize)
			})

			Step(fmt.Sprintf("wait for unscheduled resize of storage pool (%s)", unscheduledResizeTimeout), func() {
				time.Sleep(unscheduledResizeTimeout)
			})

			Step("validating and verifying size of storage pools", func() {
				ValidateStoragePoolSize(contexts, apRule.ExpectedPoolSize)
			})

			Step("validate apps", func() {
				for _, ctx := range contexts {
					ValidateContext(ctx)
				}
			})

			Step("destroy apps", func() {
				opts := make(map[string]bool)
				opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
				for _, ctx := range contexts {
					TearDownContext(ctx, opts)
				}
			})
		}
	})
})

var _ = AfterSuite(func() {
	PerformSystemCheck()
	ValidateCleanup()
})

func init() {
	ParseFlags()
}
