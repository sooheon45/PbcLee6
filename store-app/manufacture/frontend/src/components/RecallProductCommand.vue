<template>

    <v-card outlined>
        <v-card-title>
            RecallProduct
        </v-card-title>

        <v-card-text>
            <Number label="Id" v-model="value.id" :editMode="editMode"/>
            <String label="LineNum" v-model="value.lineNum" :editMode="editMode"/>
            <Number label="Qty" v-model="value.qty" :editMode="editMode"/>
            <Number label="Name" v-model="value.name" :editMode="editMode"/>
            <Number label="RecallId" v-model="value.recallId" :editMode="editMode"/>
        </v-card-text>

        <v-card-actions>
            <v-spacer></v-spacer>
            <v-btn
                    color="deep-purple lighten-2"
                    text
                    @click="recallProduct"
            >
                RecallProduct
            </v-btn>
            
            <v-btn
                    color="deep-purple lighten-2"
                    text
                    @click="close"
            >
                Close
            </v-btn>
        </v-card-actions>
    </v-card>

</template>

<script>
   
    export default {
        name: 'RecallProductCommand',
        components:{},
        props: {},
        data: () => ({
            editMode: true,
            value: {},
        }),
        created() {
            this.value.id = 0;
            this.value.lineNum = '';
            this.value.qty = 0;
            this.value.name = 0;
            this.value.recallId = 0;
        },
        watch: {
        },
        methods: {
            recallProduct() {
                this.$emit('recallProduct', this.value);
            },
            close() {
                this.$emit('closeDialog');
            },
            change() {
                this.$emit('input', this.value);
            },
        }
    }
</script>

