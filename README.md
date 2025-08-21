# install flyctl
curl -L https://fly.io/install.sh | sh
fly auth signup   # or: fly auth login

# in repo root
fly launch --now
# > choose: "Use existing Dockerfile" = yes
# > app name: e.g. tete-fastapi
# > region: pick one close to you
# It will create fly.toml and deploy.

# set secrets
fly secrets set OPENAI_API_KEY=sk-...

# scale memory (uploads + pandas + ZIP in RAM will want >512MB)
fly scale vm shared-cpu-1x --memory 1024
# if you plan 150MB zips, consider 2048:
# fly scale vm shared-cpu-1x --memory 2048