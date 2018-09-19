
# End-to-End Distributed Deep Learning Pipeline on Hops

<h1 style="color:red">This pipeline have been tested with the python dependencies:</h1>

- numpy: 1.14.5
- hops: 2.6.4
- pydoop: 2.0a3
- tensorboard: 1.8.0
- tensorflow: 1.8.0
- tensorflow-gpu: 1.8.0
- tfspark: 1.3.5

<h1 style="color:red">The notebooks assumes that you have downloaded the TinyImageNet dataset</h1>

- You can download the dataset from [here](https://tiny-imagenet.herokuapp.com/)
- Unzip the dataset in your project root so that you have the following directory layout:

```

Projects
        |
        --YourProjectName
                        |
                        --tiny-imagenet
                                      |
                                      --tiny-imagenet-200
                                                         |
                                                         --test
                                                         --train
                                                         --val
                                                         --wnids.txt
                                                         etc.
                                         
           
```
 - The same pipeline can also be used for training on the larger ImageNet dataset, you just need to change the dimensions in from 64x64x3 images to 224x224x3 and make sure that you have a similar directory layout

===

## Step 1: Parse the Raw Dataset, join Features with Labels, Save to TFRecords

[Step1_Notebook](./Step1_Convert_To_TFRecords.ipynb)

![fig-1.png](./../images/.png)

## Step 2: PreProcess Images (Data augmentation, normalization, shuffling etc)

[Step2_Notebook](./Step2_Image_PreProcessing.ipynb)

![step2.png](./../images/step2.png)


## Step 3: Single Machine Training Using Distributed Hyperparameter Search and Model Iteration using Reproducible Experiments

[Step3_Notebook](./Step3_Model_Training_Parallel_Experiments.ipynb)

![step3.png](./../images/step3.png)

## Step 4: Multiple GPU Training Using the Ring-All-Reduce Architecture

[Step4.0_Notebook](./Step4.0_Horovod_Launch.ipynb)

[Step4_Notebook](./Step4_Ring_AllReduce_GPU_Training.ipynb)

![step4.png](./../images/step4.png)


## Step 5: Model Serving

[Step5_Notebook](./Step5_Model_Serving.ipynb)

![step5.png](./../images/step5.png)