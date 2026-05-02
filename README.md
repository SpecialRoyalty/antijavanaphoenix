# Bot Telegram Railway V5

## Déploiement Railway

Variables obligatoires :

```env
BOT_TOKEN=token_botfather
DATABASE_URL=url_postgres_railway
GROUP_ID=-100xxxxxxxxxx
ADMIN_IDS=123456789
REWARD_IMAGE_URL=https://i.postimg.cc/XNzZGCZY/5475f4b9-b4f6-4fc1-b072-a9be4132adb4.jpg
REWARD_REQUIRED_JOINS=6
```

Start command Railway :

```txt
python main.py
```

Le bot doit être admin du groupe avec droits : supprimer messages, restreindre/muter, créer liens d’invitation.

## Changements V5

- Image par défaut remplacée par le lien Postimg demandé.
- Bouton groupe “Je partage” ouvre directement le privé du bot avec la bonne récompense.
- Une personne ne peut avoir qu’un challenge actif à la fois.
- Le lien d’invitation personnalisé n’est pas révoqué après 6 joins.
- Bouton admin “Image pub” ajouté.
- Chaque publication crée une récompense indépendante.
