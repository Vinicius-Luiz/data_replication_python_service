# merge na branch nova
git checkout main
git merge "<branch>"
# se houver conflitos
# git add .
# git commit -m "Resolvendo conflitos do merge"

# criando branch Localmente
git checkout -b refact/melhorando-log

# deletando branch Localmente
git branch -d "<branch>"
# deletando branch Remotamente
git push origin --delete "<branch>"
