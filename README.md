# Visualization Dashboard
A dashboard made using Streamlit to visualize plant detections. Try it out [here](https://phytooracle-dashboard.streamlit.app/).

## Guide to uploading apps to Streamlit's Community Cloud
### Creating an Account on Streamlit CC
* Open Streamlit's Community Cloud [Sign-Up Page](https://share.streamlit.io/signup), and create an account there by clicking on the **Continue With Github** button (Don't go for the other signup methods as GitHub is required to publish apps on the platform)
* Once you log in to your GitHub, Streamlit will ask you to give it some permissions. You can go through them and grant them, and if you are part of an organization, it would also prompt you to request permission to view the organization's Github repos. If the app that you want to publish is in the organization's repository, then it is **crucial** for you to grant/request these permissions. (Your organization's owners will get an email once you request them to grant access)
* Your organization should have at least one public repo to be able to grant access. 

### Setting up a Github repository for Streamlit CC
* Streamlit CC doesn't support Dockerfiles at the time of writing this ReadMe. You can still install Pip packages or libraries by specifying the name of the package and the version in a file called ```requirements.txt``` and including it in your repo. Include a ```packages.txt``` to download any dependencies you would normally install using ```apt-get install ... ``` (optional).

### Uploading the App
* Switch to the workspace that has your app repo (normally the option to do it is in the top right corner of your homepage, and you would able to toggle between your account's and organization's workspaces)
* Click on the 'New App' button (select "From an Existing Repo" if you click on the dropdown option), and specify the details of your GitHub containing the app and the subdomain you would like your deployed app to have.
* Click on 'Deploy!'

Check out the official documentation for any details that might've been missed here (https://docs.streamlit.io/streamlit-community-cloud)
