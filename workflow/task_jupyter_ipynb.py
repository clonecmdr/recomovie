from jupyter_notebook import JupyterNotebookTask

notebooks_path = '../notebooks/'

class PrepareData(JupyterNotebookTask):
    """
    A notebook that produces synthetic classification data.
    """
    notebook_path = os.path.join(notebooks_path, 'Prepare Data.ipynb')
    kernel_name = 'luigi'
    timeout = 60

    def output(self):
        return luigi.LocalTarget(os.path.join(
                                 output_path, 'model_ready_data.csv')
                                )
