from __future__ import division, absolute_import, print_function

from airflow.utils import apply_defaults

from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator


class EmrAddStepsOperatorV2(EmrAddStepsOperator):
    """Custom EmrAddStepsOperator which supports templated steps for providing execution month and year"""
    template_fields = ['job_flow_id', 'steps']  # override with steps to solve the issue above

    @apply_defaults
    def __init__(
            self,
            *args, **kwargs):
        super(EmrAddStepsOperatorV2, self).__init__(*args, **kwargs)

    def execute(self, context):
        stepids = super(EmrAddStepsOperatorV2, self).execute(context=context)
        return stepids