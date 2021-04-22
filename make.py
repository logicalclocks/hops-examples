# Load libraries
import os
import re
import fileinput
import sys
from glob import glob
import shutil



# Create path to content
path = 'notebooks/'

# Find all jupyter notebooks in all content folders
all_ipynb_files = [os.path.join(root, name)
                   for root, dirs, files in os.walk(path)
                       for name in files
                           if name.endswith((".ipynb"))]

# Remove all notebooks from checkpoint folders
ipynb_files = [ x for x in all_ipynb_files if ".ipynb_checkpoints" not in x ]

# For each file
for file in ipynb_files:
    # Convert into markdown
    os.system("jupyter nbconvert --to markdown '{file}'".format(file=file))
#    print("x")

# Get all folders in directory
folders = [x[0] for x in os.walk(path)]

# Delete .ipynb checkpoint folders
folders = [ x for x in folders if ".ipynb_checkpoints" not in x ]

# For each folder
for folder_name in folders:
    # if _files in folder name
    if '_files' in folder_name:

        # Create a new folder name
        def rchop(thestring, ending):
            if thestring.endswith(ending):
                return thestring[:-len(ending)]
            return thestring

        new_folder_name = rchop(folder_name, '_files')

        # try to rename original folder
        try:
            os.rename(folder_name, new_folder_name)
            # if error,
        except OSError:
            existing_base_folder = os.getcwd()+"/"+new_folder_name
            justcreated_base_folder = os.getcwd()+"/"+folder_name

            # get a list of all files in the folder
            generated_files = os.listdir(justcreated_base_folder)

            # copy each file to the existing folder
            for generated_file in generated_files:
                to_copy = justcreated_base_folder+"/"+generated_file
                shutil.copy(to_copy, existing_base_folder)

            # delete the newly created _files folder
            shutil.rmtree(justcreated_base_folder)

def replaceAll(file,searchExp,replaceExp):
    for line in fileinput.input(file, inplace=1):
        if searchExp in line:
            line = line.replace(searchExp,replaceExp)
        sys.stdout.write(line)

# Find all markdown files in all content folders
all_md_files = [os.path.join(root, name)
               for root, dirs, files in os.walk(path)
               for name in files
               if name.endswith((".md"))]

for file in all_md_files:
    with open(file,'r', encoding="utf-8") as f:
        filedata = f.read()
        # Find all markdown link syntaxes
        md_links = re.findall('!\\[[^\\]]+\\]\\([^)]+\\)', filedata)

        if "Redshift_" in file:
            print("Replacing images in: {0}".format(file))            
            replaceAll(file, "images/", "../images/")

        if "S3-" in file:
            print("Replacing images in: {0}".format(file))            
            replaceAll(file, "images/", "../images/")
            
        # For each markdown link
        for link in md_links:
            # Replace the full file path
            md_image_path = re.search(r'\((.*?)\)', link).group(1)
            md_image_filename = os.path.basename(md_image_path)
            md_image_title = re.search(r'\[(.*?)\]', link).group(1)

            new_link = "!["+md_image_title+"]("+md_image_path+")"
            new_link = new_link.replace("images","../images",1)
            print("Old link: {0}".format(link))
            print("New link: {0}".format(new_link))


            replaceAll(file, link, new_link)
