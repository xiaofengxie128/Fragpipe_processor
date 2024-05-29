import os
import pandas as pd
import logging
from io import StringIO
import json
import sys
import pandas
import zipfile

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.shortcuts import render
from django.core.files.storage import FileSystemStorage
from django.core.files.uploadedfile import InMemoryUploadedFile

from file_manager.models import SampleRecord, DataAnalysisQueue, ProcessingApp
logger = logging.getLogger(__name__)

# app name, must be the same as in the database
APPNAME = "FragPipe 19 processor"

# folder to store the methods
APPFOLDER = "media/primary_storage/systemfiles/fragpipe19/methods/"

# create app folder if not exist
if not os.path.exists(APPFOLDER):
    os.makedirs(APPFOLDER)

# app view for manually set-up a data analysis queue


@ login_required
def view(request):

    args = {
        'SampleRecord':
        SampleRecord.objects.order_by('-pk'),
        'WorkFlow': [f for f in os.listdir(
            APPFOLDER) if f.endswith('.workflow')],



    }

    # download link for the processor and its configration files
    processor = ProcessingApp.objects.filter(
        name=APPNAME).first().process_package.name
    if processor:
        args['download_processor'] = processor

    # selection of the workflow from uploaded or
    # existing files. save the uploaded workflow to
    # APPFOLDER for future use if save is checked
    # and new method file is uploaded
    if request.method == 'POST':
        # process method
        if (len(request.FILES) != 0 and
                request.POST.get('workflow_option') == "custom"):
            workflow_method = request.FILES['workflow_file']
            if request.POST.get('keep_method') == "True":
                fs = FileSystemStorage(location=APPFOLDER)
                fs.save(workflow_method.name, workflow_method)
        else:
            workflow_name = request.POST.get('workflow_option')
            workflow_url = APPFOLDER+workflow_name
            workflow_method = InMemoryUploadedFile(open(
                workflow_url, 'r'), None, workflow_name, None, None, None)

        update_qc = request.POST.get('replace_qc')

        # Create the manifest file for the data analysis queue, input_file_2

        result = []
        for i in range(len(request.POST.getlist('rawfile_id'))):
            file_extension = os.path.splitext(
                SampleRecord.objects.filter(
                    pk=request.POST.getlist('rawfile_id')[i]).first(
                ).newest_raw.file_location.name)[1]
            inner_list = ["ThisistempfoldeR/" +
                          request.POST.getlist('rawfile_id')[i] +
                          file_extension,
                          request.POST.getlist('experiment')[i],
                          request.POST.getlist('bioreplicate')[i],
                          request.POST.getlist('data_type')[i]]
            result.append(inner_list)

        # Create a StringIO object to write to
        file = StringIO()

        # Write the data to the StringIO object
        for row in result:
            file.write("\t".join([str(x) for x in row]))
            file.write("\n")

        # Reset the position of the StringIO object to the beginning
        file.seek(0)

        # Create an InMemoryUploadedFile from the StringIO object
        manifest_memory_file = InMemoryUploadedFile(file, None,
                                                    "input2.fp-manifest",
                                                    "text/csv",
                                                    file.tell(), None)

        newqueue = {
            "processing_name": request.POST.get('analysis_name'),
            'processing_app': ProcessingApp.objects.filter(
                name=APPNAME).first(),
            'process_creator': request.user,
            "update_qc": update_qc,
            "input_file_1": workflow_method,
            "input_file_2": manifest_memory_file,
        }


# crate a data analysis queue, attach the sample records to the queue,
# and update the quanlity check.
        newtask = DataAnalysisQueue.objects.create(**newqueue, )
        for item in request.POST.getlist('rawfile_id'):
            newtask.sample_records.add(
                SampleRecord.objects.filter(pk=item).first())
        if update_qc == "True":
            for item in newtask.sample_records.all():
                SampleRecord.objects.filter(pk=item.pk).update(
                    quanlity_check=newtask.pk)


# render the page
    return render(request,
                  'filemanager/Fragpipe_19_processor.html', args)


def auto_processing(queue_id, preset_file):
    """_This function can server two purpose:
    1. create a configure file for the processor or 3rd app to use.
    2. doing the actually data processing_
    In this app, the function is used to create a configure file based on
    setting in the preset_file (manifest.txt) to created a manifest file and
    attached to input_file_2.
    manifest.txt defines value for the following parameters:
    bioreplicate, experiment, data_type as json format.
    <bioreplicate>single element </bioreplicate> to implement list later
    <experiment> </experiment>
    <data_type> </data_type>
    ...
    """
    try:
        with zipfile.ZipFile(preset_file, "r") as archive:
            for filename in archive.namelist():
                if filename.startswith("parameters.json"):
                    with archive.open(filename) as f:
                        data = f.read()
                        parameters = json.loads(data)
    except Exception as e:
        logger.warnning("Error in reading preset file: " + str(e))

    analysisqueue = DataAnalysisQueue.objects.filter(
        pk=queue_id).first()

    manifest_result = []
    all_rawfile = analysisqueue.sample_records.all()

    for i in range(len(all_rawfile)):
        if type(parameters["data_type"]) == list:
            data_type = parameters["data_type"][i]
            experiment = parameters["experiment"][i]
            bioreplicate = parameters["bioreplicate"][i]
        else:
            data_type = parameters["data_type"]
            experiment = parameters["experiment"]
            bioreplicate = parameters["bioreplicate"]
        if experiment is None or experiment == "":
            experiment = all_rawfile[i].pk + "_"+all_rawfile[i].sample_name

        file_extension = os.path.splitext(
            SampleRecord.objects.filter(
                pk=all_rawfile[i].pk).first(
            ).newest_raw.file_location.name)[1]
        inner_list = ["ThisistempfoldeR/" + str(all_rawfile[i].pk) +
                      file_extension,
                      experiment,
                      bioreplicate,
                      data_type]
        manifest_result.append(inner_list)

    # Create a StringIO object to write to
    file = StringIO()

    # Write the data to the StringIO object
    for row in manifest_result:
        file.write("\t".join([str(x) for x in row]))
        file.write("\n")

    # Reset the position of the StringIO object to the beginning
    file.seek(0)

    # Create an InMemoryUploadedFile from the StringIO object
    manifest_memory_file = InMemoryUploadedFile(file, None,
                                                "input2.fp-manifest",
                                                "text/csv",
                                                file.tell(), None)
    analysisqueue.input_file_2 = manifest_memory_file
    analysisqueue.save()


def post_processing(queue_id):
    """_this function starts once 3rd party app finished, can be used to
    extract information for the QC numbers, etc._
    """
    """_this function starts once 3rd party app finished, can be used to
    extract information for the QC numbers, etc._
    """
    analysis_queue = DataAnalysisQueue.objects.filter(pk=queue_id).first()

    # Unique proteins for qc number 1
    if analysis_queue.output_file_1:
        df = pd.read_csv(analysis_queue.output_file_1.path, sep='\t')

        try:
            analysis_queue.output_QC_number_1 = len(df.index)
        except KeyError:
            logger.error("output_file_1 key doesn't exist")
            analysis_queue.output_QC_number_1 = 0
    else:
        analysis_queue.output_QC_number_1 = 0

    # Unique peptide for qc number 2
    if analysis_queue.output_file_2:
        df = pd.read_csv(analysis_queue.output_file_2.path, sep='\t')

        try:
            analysis_queue.output_QC_number_2 = len(df.index)
        except KeyError:
            logger.error("output_file_2 key doesn't exist")
            analysis_queue.output_QC_number_2 = 0
    else:
        analysis_queue.output_QC_number_2 = 0

    # # Unique psm for qc number 3
    # if analysis_queue.output_file_3:
    #     df = pd.read_csv(analysis_queue.output_file_3.path, sep='\t')

    #     try:
    #         analysis_queue.output_QC_number_3 = len(df.index)
    #     except KeyError:
    #         logger.error("output_file_3 error")

    # # Unique msms for qc number 4
    # if analysis_queue.output_file_4:
    #     df = pd.read_csv(analysis_queue.output_file_4.path, sep='\t')

    #     try:
    #         analysis_queue.output_QC_number_4 = len(df.index)
    #     except KeyError:
    #         logger.error("output_file_4 error")
    analysis_queue.save()
