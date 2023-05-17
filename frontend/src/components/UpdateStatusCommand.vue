<template>

    <v-card outlined>
        <v-card-title>
            UpdateStatus
        </v-card-title>

        <v-card-text>
            <Number label="Id" v-model="value.id" :editMode="editMode"/>
            <String label="ProductName" v-model="value.productName" :editMode="editMode"/>
            <String label="ProductId" v-model="value.productId" :editMode="editMode"/>
            <String label="Status" v-model="value.status" :editMode="editMode"/>
            <Number label="Qty" v-model="value.qty" :editMode="editMode"/>
            <String label="UserId" v-model="value.userId" :editMode="editMode"/>
        </v-card-text>

        <v-card-actions>
            <v-spacer></v-spacer>
            <v-btn
                    color="deep-purple lighten-2"
                    text
                    @click="updateStatus"
            >
                UpdateStatus
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
        name: 'UpdateStatusCommand',
        components:{},
        props: {},
        data: () => ({
            editMode: true,
            value: {},
        }),
        created() {
            this.value.id = 0;
            this.value.productName = '';
            this.value.productId = '';
            this.value.status = '';
            this.value.qty = 0;
            this.value.userId = '';
        },
        watch: {
        },
        methods: {
            updateStatus() {
                this.$emit('updateStatus', this.value);
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

